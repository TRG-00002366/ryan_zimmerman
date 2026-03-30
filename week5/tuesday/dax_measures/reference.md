Measure	                Formula	                                        Purpose	                                Format
Total Revenue	        SUM(FCT_ORDER_LINES[net_amount])	            Total sales amount	                    Currency
Avg Order Value         DIVIDE([Total Revenue], [Order Count], 0)       Average revenue per orders              Currency
Avf Unit Price          DIVIDE([Total Revenue], [Total Quantity], 0)    Average revenue per unit                Currency
Customer Count          DISTINCTCOUNT(FCT_ORDER_LINES[customer_key])    Number of Customers                     Whole Number
Max Order Value         MAX(FCT_ORDER_LINES[net_amount])                Highest order amount                    Currency
Min Order Value         MIN(FCT_ORDER_LINES[net_amount])                Lowest order amount                     Currency
Order Count             DISTINCTCOUNT(FCT_ORDER_LINES[order_key])       Number of unique orders                 Whole Number
Order Line Count        COUNTROWS(FCT_ORDER_LINES)                      Number of orders                        Whole Number
Product Count           DISTINCTCOUNT(FCT_ORDER_LINES[product_key])     Number of unique products               Whole Number
Revenue Per Customer    DIVIDE([Total Revenue], [Customer Count], 0)    Average revenue per customer            Currency
Sales Days              DISTINCTCOUNT(FCT_ORDER_LINES[date_key])        Number of days there have been sales    Whole Number
Total Revenue           SUM(FCT_ORDER_LINES[net_amount])                Total Revenue                           Currency
Units Per Order         DIVIDE([Total Quantity], [Order Count], 0)      Average units sold per order            Decimal