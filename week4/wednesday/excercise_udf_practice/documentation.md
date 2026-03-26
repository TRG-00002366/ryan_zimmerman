TASK 4:
Both ran in 1.2 seconds. Nonetheless, javascript will likley take longer for most applications.
It should only really be used when more complex logic is needed.

TASK 5:
FUNCTION CALC_DISCOUNT_PRICE(DECIMAL, DECIMAL); -> NUMBER
Calculates the total from a subtotal and discount percentage.
CALC_DISCOUNT_PRICE(100, 10) = 90

FUNCTION CALC_TAX(DECIMAL, DECIMAL); -> NUMBER
Calulates how much tax is oweded given the subtotal and tax rate
CALC_TAX(100, 10) = 10
FUNCTION CALC_TOTAL_WITH_TAX(DECIMAL, DECIMAL); -> NUMBER
Calculates the total from a subtotal and tax rate.
CALC_TOTAL_WITH_TAX(100, 10) = 110

FUNCTION CATEGORIZE_AMOUNT(DECIMAL); -> STRING
Given an input returns the categoru that amount is in
CATEGORIZE_AMOUNT(100) = 'Small'

FUNCTION MASK_EMAIL(STRING); -> STRING
Hides most of the email before the @ sign.
MASK_EMAIL('john.doe@example.com') = jo***@example.com

FUNCTION CLEAN_PHONE(STRING); -> STRING
formats a phone number
CLEAN_PHONE('6603418888') = (660) 341-8888

FUNCTION EXTRACT_DOMAIN(STRING); -> STRING
Extracts the domain from the email.
EXTRACT_DOMAIN('john.doe@example.com') = example.com

FUNCTION TITLE_CASE(STRING); -> STRING
Captilizes the first letter of each word, sets everything else to lower
TITLE_CASE('hEllo i am a sentance that iS nOT iN Title cASE') = Hello I Am A Sentance That Is Not In Title Case