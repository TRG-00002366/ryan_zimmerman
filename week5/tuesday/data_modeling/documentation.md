TASK 2:
ORDER_LINES is fact, it contains represents an order, has numeric qualities and has FK's to many other tables
CUSTOMER, PRODUCT, and DATE are dimensions, they don't change nearly as often as ORDER_LINES and describe an spect of the fact table.

TASK 3:
ORDER_LINES customer_key CUSTOMER customer_key many-to-1
ORDER_LINES product_key PRODUCT product_key many-to-1
ORDER_LINES date_key DATE date_key many-to-1

TASK 6:
ORDER_LINES -> CUSTOMER  many-to-1  single
ORDER_LINES ->  PRODUCT  many-to-1  single
ORDER_LINES ->  DATE     many-to-1  single

Since a star schema is being used single direction is most sensible.
This keeps the results predicatble and reflects how someone would wan't to use the report, such as finding sales of a certain product.