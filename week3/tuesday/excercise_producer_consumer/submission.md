TASK 2

==================================================
ORDER PRODUCER
==================================================
Sending 10 orders to topic 'orders-exercise'...
--------------------------------------------------
  Order ORD-001 sent to partition 1, offset 0
  Order ORD-002 sent to partition 2, offset 0
  Order ORD-003 sent to partition 1, offset 1
  Order ORD-004 sent to partition 0, offset 0
  Order ORD-005 sent to partition 0, offset 1
  Order ORD-006 sent to partition 0, offset 2
  Order ORD-007 sent to partition 1, offset 2
  Order ORD-008 sent to partition 0, offset 3
  Order ORD-009 sent to partition 1, offset 3
  Order ORD-010 sent to partition 1, offset 4
--------------------------------------------------
All 10 orders sent successfully!

==================================================
PRODUCER COMPLETE
==================================================



Task 3

==================================================
ORDER CONSUMER
==================================================

Connecting to topic 'orders-exercise'...
Waiting for orders... (Press Ctrl+C to stop)
==================================================

Received Order:
------------------------------
order_id : ORD-004
customer_id : CUST-404
product : Headphones
quantity : 1
price : 149.99
timestamp : 2026-03-10 13:39:58.376759
partition : 0
offset : 0

Received Order:
------------------------------
order_id : ORD-005
customer_id : CUST-313
product : Laptop
quantity : 4
price : 999.99
timestamp : 2026-03-10 13:39:58.378622
partition : 0
offset : 1

Received Order:
------------------------------
order_id : ORD-006
customer_id : CUST-071
product : Mouse
quantity : 1
price : 29.99
timestamp : 2026-03-10 13:39:58.380284
partition : 0
offset : 2

Received Order:
------------------------------
order_id : ORD-008
customer_id : CUST-528
product : Monitor
quantity : 2
price : 349.99
timestamp : 2026-03-10 13:39:58.384694
partition : 0
offset : 3

Received Order:
------------------------------
order_id : ORD-001
customer_id : CUST-218
product : Mouse
quantity : 5
price : 29.99
timestamp : 2026-03-10 13:39:58.246873
partition : 1
offset : 0

Received Order:
------------------------------
order_id : ORD-003
customer_id : CUST-149
product : Monitor
quantity : 1
price : 349.99
timestamp : 2026-03-10 13:39:58.375337
partition : 1
offset : 1

Received Order:
------------------------------
order_id : ORD-007
customer_id : CUST-390
product : Keyboard
quantity : 4
price : 79.99
timestamp : 2026-03-10 13:39:58.382976
partition : 1
offset : 2

Received Order:
------------------------------
order_id : ORD-009
customer_id : CUST-297
product : Headphones
quantity : 3
price : 149.99
timestamp : 2026-03-10 13:39:58.386996
partition : 1
offset : 3

Received Order:
------------------------------
order_id : ORD-010
customer_id : CUST-573
product : Laptop
quantity : 5
price : 999.99
timestamp : 2026-03-10 13:39:58.388867
partition : 1
offset : 4

Received Order:
------------------------------
order_id : ORD-002
customer_id : CUST-879
product : Keyboard
quantity : 3
price : 79.99
timestamp : 2026-03-10 13:39:58.372992
partition : 2
offset : 0


TASK 4: Run producer again
--------------------------------------------------
  Order ORD-001 sent to partition 1, offset 5
  Order ORD-002 sent to partition 2, offset 1
  Order ORD-003 sent to partition 1, offset 6
  Order ORD-004 sent to partition 0, offset 4
  Order ORD-005 sent to partition 0, offset 5
  Order ORD-006 sent to partition 0, offset 6
  Order ORD-007 sent to partition 1, offset 7
  Order ORD-008 sent to partition 0, offset 7
  Order ORD-009 sent to partition 1, offset 8
  Order ORD-010 sent to partition 1, offset 9
--------------------------------------------------

1. Most went to partition 1 and 0. Kafka partitions based on the key so it's possible most key strings where mapped to those partitions by luck.
2. No, it continues where it left off.
3. Partiton 1 received the most orders, so the last order sent to it has the highest offset of 9.
Partiton 0 highest was 7, and partition 2 only had 1 order so its highest is 1.