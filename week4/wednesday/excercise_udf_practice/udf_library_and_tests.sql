USE DATABASE RYAN_DEV_DB;
USE SCHEMA SILVER;

-- UDF: Calculate discounted price
CREATE OR REPLACE FUNCTION CALC_DISCOUNT_PRICE(
    original_price DECIMAL(10,2),
    discount_percent DECIMAL(5,2)
)
RETURNS DECIMAL(10,2)
LANGUAGE SQL
COMMENT = 'Calculates price after applying discount percentage'
AS
$$
    ROUND(original_price * (1 - discount_percent / 100), 2)
$$;


--1: CALC_TAX, takes an amount and tax rate (default 8.25%) and returns the tax amount
CREATE OR REPLACE FUNCTION CALC_TAX(
    original_price DECIMAL(10,2),
    tax_rate_percent DECIMAL(5,2) DEFAULT 8.25
)
RETURNS DECIMAL(10,2)
LANGUAGE SQL
AS
$$
    ROUND(original_price * (tax_rate_percent / 100), 2)
$$;

--2: CALC_TOTAL_WITH_TAX, returns the original amount plus tax
CREATE OR REPLACE FUNCTION CALC_TOTAL_WITH_TAX(
    original_price DECIMAL(10,2),
    tax_rate_percent DECIMAL(5,2) DEFAULT 8.25
)
RETURNS DECIMAL(10,2)
LANGUAGE SQL
AS
$$
    ROUND(original_price * (1 + tax_rate_percent / 100), 2)
$$;

--3: CATEGORIZE_AMOUNT, returns 'Micro' for amounts less than 50, 'Small' for 50-199, 'Medium' for 200-999, 'Large' for 1000+
CREATE OR REPLACE FUNCTION CATEGORIZE_AMOUNT(
    amount DECIMAL(10,2)
)
RETURNS STRING()
LANGUAGE SQL
AS
$$
    CASE
        WHEN amount < 50 THEN 'Micro'
        WHEN amount < 199 THEN 'Small'
        WHEN amount < 999 THEN 'Medium'
        ELSE 'Large'
    END
$$;


-- Test it
SELECT 
    CALC_DISCOUNT_PRICE(100.00, 10) AS ten_percent_off,
    CALC_TAX(100.00, 25) AS tax,
    CALC_TOTAL_WITH_TAX(100.00, 25) AS total,
    CATEGORIZE_AMOUNT(100.00) AS category;




--Javascript functions
-- UDF: Mask email address
CREATE OR REPLACE FUNCTION MASK_EMAIL(email STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
COMMENT = 'Masks email address for privacy (shows first 2 chars)'
AS
$$
    if (EMAIL === null || EMAIL === undefined) {
        return null;
    }
    var parts = EMAIL.split('@');
    if (parts.length !== 2) {
        return EMAIL;
    }
    var local = parts[0];
    var masked = local.substring(0, Math.min(2, local.length)) + '***@' + parts[1];
    return masked;
$$;

--1: Create a UDF called CLEAN_PHONE that:
--   Removes all non-digit characters
--   Formats as (XXX) XXX-XXXX if 10 digits
--   Returns original if not 10 digits
CREATE OR REPLACE FUNCTION CLEAN_PHONE(phone_number STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    var digits = PHONE_NUMBER.replace(/\\D/g, '');
    if (digits.length === 10) {
        return '(' + digits.substr(0,3) + ') ' + digits.substr(3,3) + '-' + digits.substr(6,4);
    } else {
        return PHONE_NUMBER;
    }
$$;


--2: Create a UDF called EXTRACT_DOMAIN that extracts the domain from an email address (e.g., 'example.com' from 'user@example.com').
CREATE OR REPLACE FUNCTION EXTRACT_DOMAIN(email STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    if (EMAIL && EMAIL.indexOf('@') !== -1) {
        return EMAIL.split('@')[1];
    } else {
        return null;
    }
$$;


--3: Create a UDF called TITLE_CASE that converts a string to title case (first letter of each word capitalized).
CREATE OR REPLACE FUNCTION TITLE_CASE(input STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    if (!INPUT) return INPUT;
    return INPUT.toLowerCase().split(' ').map(word => word.charAt(0).toUpperCase() + word.slice(1)).join(' ');
$$;


-- Test it
SELECT 
    MASK_EMAIL('john.doe@example.com') AS MASK_EMAIL,
    CLEAN_PHONE('6603418888') AS CLEAN_PHONE,
    EXTRACT_DOMAIN('john.doe@example.com') AS EXTRACT_DOMAIN,
    TITLE_CASE('hEllo i am a sentance that iS nOT iN Title cASE') AS TITLE_CASE;
    


-- Apply to sample data
USE DATABASE SNOWFLAKE_SAMPLE_DATA;
USE SCHEMA TPCH_SF1;

-- Use your UDFs with customer data
SELECT 
    C_CUSTKEY,
    C_NAME,
    RYAN_DEV_DB.SILVER.CATEGORIZE_AMOUNT(C_ACCTBAL) AS balance_category,
    C_ACCTBAL,
    RYAN_DEV_DB.SILVER.CALC_TAX(C_ACCTBAL, 8.25) AS estimated_tax
FROM CUSTOMER
LIMIT 20;

USE DATABASE RYAN_DEV_DB;
USE SCHEMA SILVER;

CREATE OR REPLACE VIEW V_CUSTOMER_ENHANCED AS
SELECT 
    C_CUSTKEY AS customer_id,
    C_NAME AS customer_name,
    CATEGORIZE_AMOUNT(C_ACCTBAL) AS account_tier,
    C_ACCTBAL AS balance,
    CALC_TAX(C_ACCTBAL, 8.25) AS tax_liability,
    MASK_EMAIL(C_NAME || '@company.com') AS masked_email
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER;

-- Test the view
SELECT * FROM V_CUSTOMER_ENHANCED LIMIT 10;

-- Create equivalent functions
CREATE OR REPLACE FUNCTION DOUBLE_SQL(x NUMBER)
RETURNS NUMBER
LANGUAGE SQL
AS $$ x * 2 $$;

CREATE OR REPLACE FUNCTION DOUBLE_JS(x FLOAT)
RETURNS FLOAT
LANGUAGE JAVASCRIPT
AS $$ return X * 2; $$;

-- Time comparison on large dataset
-- Run each and note the execution time from Query History

-- SQL UDF
SELECT DOUBLE_SQL(C_ACCTBAL) FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER;

-- JavaScript UDF
SELECT DOUBLE_JS(C_ACCTBAL) FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER;

-- List your UDFs
SHOW USER FUNCTIONS IN SCHEMA SILVER;

-- Describe a specific UDF
DESCRIBE FUNCTION CALC_DISCOUNT_PRICE(DECIMAL, DECIMAL);
DESCRIBE FUNCTION CALC_TAX(DECIMAL, DECIMAL);
DESCRIBE FUNCTION CALC_TOTAL_WITH_TAX(DECIMAL, DECIMAL);
DESCRIBE FUNCTION CATEGORIZE_AMOUNT(DECIMAL);

DESCRIBE FUNCTION MASK_EMAIL(STRING);
DESCRIBE FUNCTION CLEAN_PHONE(STRING);
DESCRIBE FUNCTION EXTRACT_DOMAIN(STRING);
DESCRIBE FUNCTION TITLE_CASE(STRING);