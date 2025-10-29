import {expect, test, describe, beforeAll, afterAll} from "bun:test";
import duckdb from "duckdb";
import fs from "fs";
import path from "path";

let db;

// Read the anonymize macro SQL
const anonymizeMacroSql = fs.readFileSync(
  path.join(import.meta.dir, "../templates/anonymize.sql"),
  "utf-8"
);

function createTestDb() {
  return new Promise((resolve, reject) => {
    const database = new duckdb.Database(':memory:');
    database.all(anonymizeMacroSql, (err) => {
      if (err) return reject(err);
      resolve(database);
    });
  });
}

/**
 * Helper to execute a query and return results
 */
function executeQuery(database, query) {
  return new Promise((resolve, reject) => {
    database.all(query, (err, res) => {
      if (err) return reject(err);
      resolve(res);
    });
  });
}

beforeAll(async () => {
  db = await createTestDb();
});

afterAll((done) => {
  db.close(() => done());
});

describe("anon_is_usa - detect US country codes", () => {
  
  test("case-insensitive country matching for USA variations", async () => {
    const query = `
      SELECT 
        anon_is_usa('USA') AS uppercase_usa,
        anon_is_usa('usa') AS lowercase_usa,
        anon_is_usa('US') AS uppercase_us,
        anon_is_usa('us') AS lowercase_us,
        anon_is_usa('United States') AS united_states,
        anon_is_usa('united states of america') AS full_name,
        anon_is_usa('U.S.') AS uppercase_us_periods,
        anon_is_usa('u.s.') AS lowercase_us_periods,
        anon_is_usa('U.S.A') AS uppercase_usa_periods,
        anon_is_usa('u.s.a') AS lowercase_usa_periods,
        anon_is_usa('U.S.A.') AS uppercase_usa_periods_full
    `;
    const result = await executeQuery(db, query);
    expect(result[0].uppercase_usa).toBe(true);
    expect(result[0].lowercase_usa).toBe(true);
    expect(result[0].uppercase_us).toBe(true);
    expect(result[0].lowercase_us).toBe(true);
    expect(result[0].united_states).toBe(true);
    expect(result[0].full_name).toBe(true);
    expect(result[0].uppercase_us_periods).toBe(true);
    expect(result[0].lowercase_us_periods).toBe(true);
    expect(result[0].uppercase_usa_periods).toBe(true);
    expect(result[0].lowercase_usa_periods).toBe(true);
    expect(result[0].uppercase_usa_periods_full).toBe(true);
  });

  test("non-US countries return false", async () => {
    const query = `
      SELECT 
        anon_is_usa('Canada') AS canada,
        anon_is_usa('UK') AS uk,
        anon_is_usa('France') AS france,
        anon_is_usa('Mexico') AS mexico,
        anon_is_usa('Germany') AS germany
    `;
    const result = await executeQuery(db, query);
    expect(result[0].canada).toBe(false);
    expect(result[0].uk).toBe(false);
    expect(result[0].france).toBe(false);
    expect(result[0].mexico).toBe(false);
    expect(result[0].germany).toBe(false);
  });

  test("null country returns false", async () => {
    const query = `
      SELECT anon_is_usa(NULL) AS result
    `;
    const result = await executeQuery(db, query);
    expect(result[0].result).toBe(false);
  });

  test("empty string returns false", async () => {
    const query = `
      SELECT anon_is_usa('') AS result
    `;
    const result = await executeQuery(db, query);
    expect(result[0].result).toBe(false);
  });
});

describe("anon_zip - anonymize US ZIP codes", () => {
  
  test("normal population 3-digit ZIP codes", async () => {
    const query = `
      SELECT 
        anon_zip('90210') AS zip_902,
        anon_zip('10001') AS zip_100,
        anon_zip('60601') AS zip_606,
        anon_zip('12345') AS zip_123,
        anon_zip('62701') AS zip_627
    `;
    const result = await executeQuery(db, query);
    expect(result[0].zip_902).toBe('902');
    expect(result[0].zip_100).toBe('100');
    expect(result[0].zip_606).toBe('606');
    expect(result[0].zip_123).toBe('123');
    expect(result[0].zip_627).toBe('627');
  });

  test("low-population 3-digit ZIP codes return NULL", async () => {
    const query = `
      SELECT 
        anon_zip('03601') AS zip_036,
        anon_zip('05901') AS zip_059,
        anon_zip('10201') AS zip_102,
        anon_zip('20301') AS zip_203,
        anon_zip('20501') AS zip_205,
        anon_zip('82301') AS zip_823,
        anon_zip('89301') AS zip_893
    `;
    const result = await executeQuery(db, query);
    // All low-population ZIP codes should return NULL
    expect(result[0].zip_036).toBeNull();
    expect(result[0].zip_059).toBeNull();
    expect(result[0].zip_102).toBeNull();
    expect(result[0].zip_203).toBeNull();
    expect(result[0].zip_205).toBeNull();
    expect(result[0].zip_823).toBeNull();
    expect(result[0].zip_893).toBeNull();
  });

  test("ZIP codes with extra formatting", async () => {
    const query = `
      SELECT 
        anon_zip('12345-6789') AS zip_plus_four,
        anon_zip('123456789') AS nine_digits
    `;
    const result = await executeQuery(db, query);
    // Should extract first 3 digits even with extra formatting
    expect(result[0].zip_plus_four).toBe('123');
    expect(result[0].nine_digits).toBe('123');
  });

  test("short or invalid ZIP codes return NULL", async () => {
    const query = `
      SELECT 
        anon_zip('12') AS too_short,
        anon_zip('1') AS one_digit,
        anon_zip('1234') AS four_digits,
        anon_zip('') AS empty_string,
        anon_zip(NULL) AS null_value
    `;
    const result = await executeQuery(db, query);
    expect(result[0].too_short).toBeNull();
    expect(result[0].one_digit).toBeNull();
    expect(result[0].four_digits).toBeNull();
    expect(result[0].empty_string).toBeNull();
    expect(result[0].null_value).toBeNull();
  });

  test("non-numeric ZIP codes return NULL", async () => {
    const query = `
      SELECT 
        anon_zip('M5H 2N2') AS canadian,
        anon_zip('SW1A 1AA') AS uk,
        anon_zip('ABCDE') AS letters
    `;
    const result = await executeQuery(db, query);
    expect(result[0].canadian).toBeNull();
    expect(result[0].uk).toBeNull();
    expect(result[0].letters).toBeNull();
  });

  test("all low-population ZIP prefixes from census data", async () => {
    const query = `
      SELECT 
        anon_zip('36901') AS zip_369,
        anon_zip('55601') AS zip_556,
        anon_zip('69201') AS zip_692,
        anon_zip('82101') AS zip_821,
        anon_zip('87801') AS zip_878,
        anon_zip('87901') AS zip_879,
        anon_zip('88401') AS zip_884
    `;
    const result = await executeQuery(db, query);
    expect(result[0].zip_369).toBeNull();
    expect(result[0].zip_556).toBeNull();
    expect(result[0].zip_692).toBeNull();
    expect(result[0].zip_821).toBeNull();
    expect(result[0].zip_878).toBeNull();
    expect(result[0].zip_879).toBeNull();
    expect(result[0].zip_884).toBeNull();
  });
});

describe("anonymize dates", () => {
  
  test("full datetime with timezone", async () => {
    const query = `
      SELECT 
        anon_date_to_year('2023-10-15T14:30:00+00:00') AS with_timezone,
        anon_date_to_year('2023-10-15T14:30:00Z') AS with_z,
        anon_date_to_year('2023-10-15T14:30:00.123+05:30') AS with_milliseconds
    `;
    const result = await executeQuery(db, query);
    expect(result[0].with_timezone).toBe('2023');
    expect(result[0].with_z).toBe('2023');
    expect(result[0].with_milliseconds).toBe('2023');
  });

  test("date only formats", async () => {
    const query = `
      SELECT 
        anon_date_to_year('2023-10-15') AS full_date,
        anon_date_to_year('2023-10') AS year_month,
        anon_date_to_year('2023') AS year_only
    `;
    const result = await executeQuery(db, query);
    expect(result[0].full_date).toBe('2023');
    expect(result[0].year_month).toBe('2023');
    expect(result[0].year_only).toBe('2023');
  });

  test("various year values", async () => {
    const query = `
      SELECT 
        anon_date_to_year('1990-05-20') AS year_1990,
        anon_date_to_year('2000-01-01') AS year_2000,
        anon_date_to_year('2024-12-31T23:59:59Z') AS year_2024,
        anon_date_to_year('1875-03-15') AS year_1875
    `;
    const result = await executeQuery(db, query);
    expect(result[0].year_1990).toBe('1990');
    expect(result[0].year_2000).toBe('2000');
    expect(result[0].year_2024).toBe('2024');
    expect(result[0].year_1875).toBe('1875');
  });

  test("null value", async () => {
    const query = `
      SELECT anon_date_to_year(NULL) AS result
    `;
    const result = await executeQuery(db, query);
    expect(result[0].result).toBeNull();
  });

  test("empty string", async () => {
    const query = `
      SELECT anon_date_to_year('') AS result
    `;
    const result = await executeQuery(db, query);
    expect(result[0].result).toBeNull();
  });

  test("strings too short to contain a year", async () => {
    const query = `
      SELECT 
        anon_date_to_year('202') AS three_chars,
        anon_date_to_year('20') AS two_chars,
        anon_date_to_year('2') AS one_char
    `;
    const result = await executeQuery(db, query);
    expect(result[0].three_chars).toBeNull();
    expect(result[0].two_chars).toBeNull();
    expect(result[0].one_char).toBeNull();
  });

  test("datetime without timezone", async () => {
    const query = `
      SELECT 
        anon_date_to_year('2023-10-15T14:30:00') AS without_tz,
        anon_date_to_year('2023-10-15 14:30:00') AS space_separator
    `;
    const result = await executeQuery(db, query);
    expect(result[0].without_tz).toBe('2023');
    expect(result[0].space_separator).toBe('2023');
  });

  test("edge case years", async () => {
    const query = `
      SELECT 
        anon_date_to_year('0001-01-01') AS year_0001,
        anon_date_to_year('9999-12-31') AS year_9999
    `;
    const result = await executeQuery(db, query);
    expect(result[0].year_0001).toBe('0001');
    expect(result[0].year_9999).toBe('9999');
  });

  test("partial precision dates (FHIR spec)", async () => {
    const query = `
      SELECT 
        anon_date_to_year('2023') AS year_precision,
        anon_date_to_year('2023-10') AS month_precision,
        anon_date_to_year('2023-10-15') AS day_precision
    `;
    const result = await executeQuery(db, query);
    // All should return just the year
    expect(result[0].year_precision).toBe('2023');
    expect(result[0].month_precision).toBe('2023');
    expect(result[0].day_precision).toBe('2023');
  });
});

describe("anonymize strings", () => {
  
  test("redact regular string", async () => {
    const query = `
      SELECT anon_redact_string('John Doe') AS result
    `;
    const result = await executeQuery(db, query);
    expect(result[0].result).toBe('*****');
  });

  test("redact various string lengths", async () => {
    const query = `
      SELECT 
        anon_redact_string('a') AS single_char,
        anon_redact_string('short') AS short_string,
        anon_redact_string('This is a longer string with spaces') AS long_string,
        anon_redact_string('123-45-6789') AS ssn_format
    `;
    const result = await executeQuery(db, query);
    // All strings should be replaced with exactly five asterisks
    expect(result[0].single_char).toBe('*****');
    expect(result[0].short_string).toBe('*****');
    expect(result[0].long_string).toBe('*****');
    expect(result[0].ssn_format).toBe('*****');
  });

  test("redact null value", async () => {
    const query = `
      SELECT anon_redact_string(NULL) AS result
    `;
    const result = await executeQuery(db, query);
    expect(result[0].result).toBeNull();
  });

  test("redact empty string", async () => {
    const query = `
      SELECT anon_redact_string('') AS result
    `;
    const result = await executeQuery(db, query);
    expect(result[0].result).toBe('*****');
  });

  test("redact strings with special characters", async () => {
    const query = `
      SELECT 
        anon_redact_string('email@example.com') AS email,
        anon_redact_string('(555) 123-4567') AS phone,
        anon_redact_string('$1,000.00') AS currency,
        anon_redact_string('Test\nWith\nNewlines') AS newlines
    `;
    const result = await executeQuery(db, query);
    expect(result[0].email).toBe('*****');
    expect(result[0].phone).toBe('*****');
    expect(result[0].currency).toBe('*****');
    expect(result[0].newlines).toBe('*****');
  });

  test("redact numeric strings", async () => {
    const query = `
      SELECT 
        anon_redact_string('12345') AS numbers,
        anon_redact_string('3.14159') AS decimal
    `;
    const result = await executeQuery(db, query);
    expect(result[0].numbers).toBe('*****');
    expect(result[0].decimal).toBe('*****');
  });
});