


create database ride_hailing_analysis;
use ride_hailing_analysis;

select * from Bookings

/*🚖 Ride Hailing Analytics Project (OLA / Uber)*/

/*
STAR METHOD OVERVIEW – RIDE HAILING ANALYSIS

Situation: High cancellations and poor revenue visibility due to unstructured data
Task: Clean data, design star schema, and perform SQL analysis
Action: Normalized bookings into fact & dimension tables and built KPI-driven queries
Result: Actionable insights on cancellations, revenue trends, and demand patterns
*/



/* 
STAR SCHEMA DESIGN – RIDE HAILING ANALYSIS

Fact Table:
- Bookings (transaction-level ride data)

Dimension Tables:
- Customers (unique riders)
- Vehicles (vehicle categories)
- Locations (pickup & drop points)
- Payments (payment methods & booking value)

This star schema reduces redundancy, improves query performance,
and enables scalable analytical reporting similar to OLA/Uber systems.
*/

CREATE TABLE Customers (customer_key INT IDENTITY(1,1) primary key,
    customer_id VARCHAR(50) 
);


INSERT INTO Customers (customer_id)
SELECT DISTINCT Customer_ID
FROM dbo.Bookings
WHERE Customer_ID IS NOT NULL;
 
-- Total rows in Booking table is 103025 and total rows in customer table is 94544 to know if there is any 
--repetitive customers with same customers id

SELECT COUNT(*) AS repeat_customers
FROM (
    SELECT Customer_ID
    FROM dbo.Bookings
    GROUP BY Customer_ID
    HAVING COUNT(*) > 1
) t;

select * from Bookings

--vehicle type
CREATE TABLE Vehicles (
    vehicle_key INT IDENTITY(1,1) PRIMARY KEY,
    vehicle_type VARCHAR(50) UNIQUE
);

INSERT INTO Vehicles (vehicle_type)
SELECT DISTINCT Vehicle_Type
FROM dbo.Bookings;

select * from Vehicles

--location
CREATE TABLE Dim_Locations (
    location_key INT IDENTITY(1,1) PRIMARY KEY,
    location_name VARCHAR(100) UNIQUE NOT NULL
);


-- Insert unique pickup locations
INSERT INTO Dim_Locations (location_name)
SELECT DISTINCT Pickup_Location
FROM Bookings
WHERE Pickup_Location IS NOT NULL;

-- Insert unique drop locations not already in Dim_Locations
INSERT INTO Dim_Locations (location_name)
SELECT DISTINCT Drop_Location
FROM Bookings
WHERE Drop_Location IS NOT NULL
  AND Drop_Location NOT IN (SELECT location_name FROM Dim_Locations);

select * from Dim_Locations

--pyment
CREATE TABLE Payment (
    payment_key INT IDENTITY(1,1) PRIMARY KEY,
    payment_method VARCHAR(50) UNIQUE NOT NULL
);

INSERT INTO Payment (payment_method)
SELECT DISTINCT 
    COALESCE(Payment_Method, 'Unknown') AS payment_method
FROM Bookings;

select * from Payment

--Fact Table
CREATE TABLE Fact_Bookings (
    booking_id nvarchar(50) PRIMARY KEY,
    pickup_location_key INT FOREIGN KEY REFERENCES Locations(location_key),
    drop_location_key INT FOREIGN KEY REFERENCES Locations(location_key),
    customer_key INT FOREIGN KEY REFERENCES Customers(customer_key),
    vehicle_key INT FOREIGN KEY REFERENCES Vehicles(vehicle_key),
    booking_value DECIMAL(10,2),
    booking_status VARCHAR(50),
    booking_date DATE
);


INSERT INTO Fact_Bookings (
    booking_id,
    pickup_location_key,
    drop_location_key,
    customer_key,
    vehicle_key,
    booking_value,
    booking_status,
    booking_date
)
SELECT 
    b.Booking_ID,
    pl.location_key,
    dl.location_key,
    c.customer_key,
    v.vehicle_key,
    b.Booking_Value,
    b.Booking_Status,
    b.Date
FROM Bookings AS b
INNER JOIN Customers AS c 
    ON b.Customer_ID = c.customer_id
INNER JOIN Vehicles AS v 
    ON b.Vehicle_Type = v.vehicle_type
INNER JOIN Locations AS pl 
    ON b.Pickup_Location = pl.location_name
INNER JOIN Locations AS dl 
    ON b.Drop_Location = dl.location_name;

select * from Fact_Bookings

-- How many total bookings exist in the fact table vs original table?
create view total_bookings as select count(*) as  total_bookings_in_fact
from Fact_Bookings;

select count(*) as  total_bookings_in_original
from Bookings;

----How does booking volume vary by pickup location?

create view bookings_per_pickup_location as
select l.location_name as pickup_location ,count(*) as total_bookings
from Fact_Bookings f
join Locations l on f.pickup_location_key = l.location_key
group by l.location_name

SELECT * 
FROM bookings_per_pickup_location
ORDER BY total_bookings DESC;

--Revenue by vehicle type

create view Renvenue_by_vehicle_type as select  v.vehicle_type, SUM(f.booking_value) AS total_revenue from Fact_Bookings f
JOIN Vehicles v ON f.vehicle_key = v.vehicle_key
group by v.vehicle_type

--Total Ride cancellations
select count(*) as cancelled_rides from Fact_Bookings where booking_status='Canceled by Customer'
select count(*) as cancelled_rides from Fact_Bookings where booking_status='Canceled by Driver'

--How many rides were completed vs cancelled?


CREATE VIEW vw_completed_vs_cancelled AS
SELECT
    SUM(
        CASE 
            WHEN booking_status LIKE 'Canceled%' THEN 1 
            ELSE 0 
        END
    ) AS cancelled_rides,

    SUM(
        CASE 
            WHEN booking_status NOT LIKE 'Canceled%' THEN 1 
            ELSE 0 
        END
    ) AS completed_rides
FROM Fact_Bookings;

--Revenue by payment method
create view Payment_Method_Share as select p.payment_method ,sum(booking_value) as Total_revenue from Fact_Bookings f
join payment p on f.payment_key =p.payment_key
group by p.payment_method

select * from Fact_Bookings
select * from Payment

--What is the cancellation rate (%)?

create view cancellation_rate as select  cast(100.0* sum (case when booking_status like 'canceled%' then 1 else 0 end)
/count(*) as decimal(5,2) ) as cancellation_rate_pct from Fact_Bookings

--Which pickup locations have the highest demand?

create view Bookings_by_pickup_Locations as select l.location_name as pickup_location,count(*) as total_bookings from Fact_Bookings f
join Locations l on f.pickup_location_key=l.location_key
group by l.location_name 

--Which pickup → drop routes are most frequently used?

 create view Pickup_Drop_Route as select p.location_name as pickup_location ,d.location_name as drop_location ,count(*) as total_rides 
from Fact_Bookings f
join Locations p on f.pickup_location_key=p.location_key
join Locations d on f.drop_location_key= d.location_key
group by p.location_name,d.location_name 

select * from Locations
select * from Fact_Bookings

--Which locations experience the highest ride cancellations?
create view Location_with_highest_cancellation as
	select p.location_name ,count(*) as highest_cancelled_ride from Fact_Bookings f
	join Locations p on f.drop_location_key=p.location_key
	join Locations d on f.pickup_location_key=d.location_key
	where booking_status like 'canceled%' 
	group by p.location_name 

	select d.location_name ,count(*) as highest_cancelled_ride from Fact_Bookings f
	join Locations p on f.drop_location_key=p.location_key
	join Locations d on f.pickup_location_key=d.location_key
	where booking_status like 'canceled%' 
	group by d.location_name 
	order by highest_cancelled_ride desc

/* B. Revenue Analysis

What is the total revenue generated?

What is the average booking value?

How does revenue vary by vehicle type?

Which vehicle type generates the highest revenue?

What is the revenue contribution of each payment method?

What is the average booking value per vehicle type?

Which pickup locations generate the most revenue?

Which pickup → drop routes generate the highest revenue? */


--What is the total revenue generated?
create view total_revenue as select sum(booking_value) as total_revenue  from Fact_Bookings

--What is the average booking value?
select avg(booking_value) as avg_booking_val from Fact_Bookings

--How does revenue vary by vehicle type?
create view revenue_by_vehicle_type as select sum(booking_value) as total_revenue, v.vehicle_type from Fact_Bookings f
join Vehicles v on f.vehicle_key=v.vehicle_key
group by v.vehicle_type 

--What is the revenue contribution of each payment method?
create view  Revenue_by_Payment_Methods as select sum(booking_value) as total_revenue ,p.payment_method from Fact_Bookings f
join Payment p on f.payment_key=p.payment_key
group by p.payment_method

--What is the average booking value per vehicle type?
select avg(booking_value) as avg_booking_value ,v.vehicle_type from Fact_Bookings f
join Vehicles v on f.vehicle_key=v.vehicle_key
group by v.vehicle_type
order by avg_booking_value desc

--Which pickup → drop routes generate the highest revenue? */
create view revenue_by_pickup_loc as select l.location_name ,sum(booking_value) as total_revenue from Fact_Bookings f
join Locations l on f.pickup_location_key=l.location_key
group by l.location_name

select l.location_name ,sum(booking_value) as total_revenue from Fact_Bookings f
join Locations l on f.drop_location_key=l.location_key
group by l.location_name
order by total_revenue desc


/*C. Cancellation Analysis 

How many rides were cancelled by customers?

How many rides were cancelled by drivers?

What percentage of total bookings are cancelled?

Which vehicle types have the highest cancellation rates?

Which payment methods are associated with higher cancellations?

Which pickup locations experience the most cancellations?

Are cancellations more frequent for specific pickup → drop routes?*/

--How many rides were cancelled by customers?
select count(*) as cancelled_rides_by_customers from Fact_Bookings
where booking_status ='canceled by customer'

--How many rides were cancelled by drivers?
select count(*) as cancelled_rides_by_customers from Fact_Bookings
where booking_status='canceled by Driver'

--What percentage of total bookings are cancelled?
create view cancellation_percent as SELECT 
    COUNT(*) AS total_bookings,
	SUM(CASE WHEN booking_status LIKE 'Canceled%' THEN 1 ELSE 0 
        END) AS cancelled_bookings,
	CAST(
        100.0 *
        SUM(CASE WHEN booking_status LIKE 'Canceled%' THEN 1 ELSE 0 END)
        / COUNT(*) 
        AS DECIMAL(5,2)
    ) AS cancellation_percentage FROM Fact_Bookings;

--Which vehicle types have the highest cancellation rates?
create view cancellation_by_vehicle as select v.vehicle_type ,count(*)  as cancelled_rides from Fact_Bookings f
join Vehicles v on f.vehicle_key=v.vehicle_key
where booking_status like 'canceled%' 
group by v.vehicle_type

--Which payment methods are associated with higher cancellations?
select p.payment_method,count(*) AS cancelled_rides from Fact_Bookings f
join Payment p ON f.payment_key = p.payment_key
where booking_status like 'canceled%' 
group by p.payment_method

--Which pickup locations experience the most cancellations?
create view cancellation_by_location as select  l.location_name,count(*) AS cancelled_rides from Fact_Bookings f
join Locations l ON f.pickup_location_key = l.location_key
where booking_status like 'canceled%' 
group by l.location_name

/*D. Customer Behavior Analysis

How many unique customers are there?

How many bookings does each customer make?

Who are the top customers by total bookings?

Who are the top customers by total spend?

What percentage of customers cancel rides frequently?

Are repeat customers more likely to complete rides?

What is the average booking value per customer?*/

--How many unique customers are there?
create view unique_customer as select distinct(count(*)) AS total_customers
FROM Customers;

--How many bookings does each customer make?
select c.customer_id,count(*) as total_bookings from Fact_Bookings f
join Customers c ON f.customer_key = c.customer_key
group by c.customer_id;

--Who are the top customers by total spent?
create view top_10_customer as select top 10 c.customer_id, sum(f.booking_value) as total_spent
from Fact_Bookings f
join Customers c ON f.customer_key = c.customer_key
group by c.customer_id
order by total_spent desc;

--What percentage of customers cancel rides frequently?
create view customer_cancel_freq as
SELECT 
    c.customer_id,
    COUNT(*) AS cancelled_rides
FROM Fact_Bookings f
JOIN Customers c ON f.customer_key = c.customer_key
WHERE booking_status LIKE 'Canceled%'
GROUP BY c.customer_id
HAVING COUNT(*) > 1

--Are repeat customers more likely to complete rides?
create view total_booking_vs_completed as SELECT 
    c.customer_id,
    COUNT(*) AS total_bookings,
    SUM(
        CASE 
            WHEN f.booking_status NOT LIKE 'Cancelled%' THEN 1 
            ELSE 0 
        END
    ) AS completed_rides
FROM Fact_Bookings f
JOIN Customers c 
    ON f.customer_key = c.customer_key
GROUP BY c.customer_id;

/*E. Vehicle Performance Analysis

How many bookings are made per vehicle type?

Which vehicle types are under-utilized?

Which vehicle types have the highest cancellation rates?

What is the average revenue per vehicle type?

Which vehicle types perform best at high-demand locations?*/

--How many bookings are made per vehicle type?
create view booking_by_vehicle as select count(*) as total_bookings ,v.vehicle_type from Fact_Bookings f
join Vehicles v on f.vehicle_key=v.vehicle_key
group by v.vehicle_type
order by total_bookings desc

--Which vehicle types are under-utilized?

Select 
    v.vehicle_type,
    COUNT(*) AS total_bookings
FROM Fact_Bookings f
JOIN Vehicles v 
    ON f.vehicle_key = v.vehicle_key
GROUP BY v.vehicle_type
ORDER BY total_bookings ASC;

--Which vehicle types have the highest cancellation rates?
create view cancellation_rate_by_vehicle as
select v.vehicle_type, count(*) AS total_bookings, sum(case when f.booking_status like 'Canceled%' then 1  else 0 end) AS cancelled_bookings,
cast(100.0 * sum(case when f.booking_status LIKE 'Canceled%' then 1 else 0 end)/ count(*) as decimal (5,2)) AS cancellation_rate
FROM Fact_Bookings f
JOIN Vehicles v on f.vehicle_key = v.vehicle_key
group by v.vehicle_type
order by cancellation_rate desc;

--What is the average revenue per vehicle type?

create view avg_revenue_by_vehicle as select avg(f.booking_value) as avg_revenue,v.vehicle_type from Fact_Bookings f
join Vehicles v on f.vehicle_key=v.vehicle_key
group by v.vehicle_type

/*F. Payment Method Analysis

How many bookings are made using each payment method?

Which payment method generates the highest revenue?

Which payment methods have higher cancellation rates?

Are certain payment methods preferred in high-value bookings?

How many bookings used an “Unknown” payment method?*/

--How many bookings are made using each payment method?
create view bookings_by_payment as select count(f.booking_id) as total_bookings  ,p.payment_method from Fact_Bookings f
join Payment p on f.payment_key=p.payment_key
group by p.payment_method

--Which payment method generates the highest revenue?
select p.payment_method , sum(f.booking_value) from Fact_Bookings f
join Payment p on f.payment_key=p.payment_key
group by p.payment_method

--Are certain payment methods preferred in high-value bookings?
create view payment_by_high_value_bookings as select p.payment_method ,count(*)  as high_val_booking from Fact_Bookings f
join Payment p on f.payment_key = p.payment_key where f.booking_value>(
select avg(booking_value) from Fact_Bookings)
group by p.payment_method
order by high_val_booking desc

--How many bookings used an “Unknown” payment method?

create view unknown_payment as select count(*) as unknown_payment_bookings from Fact_Bookings f
join Payment p on f.payment_key=p.payment_key
where p.payment_method='Unknown'

/* Data Quality & Operations (Interview Gold)

How many bookings had missing payment information?

How many bookings required data cleaning or standardization?

Why was a star schema chosen over a single flat table?

How does this model support scalability and reporting?*/

--How many bookings had missing payment information?
select count(*) as unknown_payment from Fact_Bookings f
join Payment p on f.payment_key=p.payment_key where p.payment_method='Unknown'

--How many bookings required data cleaning or standardization?
SELECT 
    COUNT(DISTINCT f.booking_id) AS bookings_needing_cleaning
FROM Fact_Bookings f
LEFT JOIN Payment p 
    ON f.payment_key = p.payment_key
WHERE 
    p.payment_method = 'Unknown'
    OR f.booking_value IS NULL
    OR f.booking_value <= 0
    OR f.booking_status IN (
        'Canceled',
        'Canceled',
        'canceled by driver',
        'canceled by customer'
    );


--Why was a star schema chosen over a single flat table?
/*A star schema was chosen to reduce redundancy, improve data clarity, and make analytical 
queries simpler and faster compared to a single flat table*/

select * from Bookings
select * from Locations
select * from Vehicles
select * from Fact_Bookings
select count(*) from Bookings where date like '2024-07-02%'

/*
Project Outcome:
This data model and analysis enabled the business to:
- Identify key cancellation drivers
- Analyze revenue by vehicle, location, and payment method
- Improve visibility into customer behavior
- Build scalable dashboards in Power BI
*/
