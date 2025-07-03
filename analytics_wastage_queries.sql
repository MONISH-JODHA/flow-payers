begin
------ EC2 hourly wastage-------------------

CREATE OR REPLACE TEMPORARY TABLE ec2_payer_max_date as
select
bill_payer_account_id as bill_payeraccountid,
DATEADD(SECOND, -1, max(line_item_usage_start_date)) AS MAX_DATE
from analytics_application_table_#startyear_#startmonth t,
      LATERAL FLATTEN(input => t.product:key_value) f
WHERE
t.line_item_line_item_type = 'DiscountedUsage' 
and f.value:key in ('product_name')
AND f.value:value = 'Amazon Elastic Compute Cloud'
and bill_payer_account_id in (#payers_ids)
group by 1;


CREATE or replace TEMPORARY TABLE ec2_ri_report_hourly_temp AS
WITH rifee_data AS (

SELECT DISTINCT
    t.reservation_reservation_a_r_n,
    SPLIT_PART(t.LINE_ITEM_USAGE_TYPE, ':', 2) AS instance_type,
    case
 when t.LINE_ITEM_USAGE_TYPE like '%.nano%' then 0.25
                            when t.LINE_ITEM_USAGE_TYPE like '%.micro%'  then 0.5
                            when t.LINE_ITEM_USAGE_TYPE like '%.small%'  then 1
                            when t.LINE_ITEM_USAGE_TYPE like '%.medium%'  then 2
                            when t.LINE_ITEM_USAGE_TYPE like '%.16xl%'  then 128
                            when t.LINE_ITEM_USAGE_TYPE like '%.18xl%' then 144
                            when t.LINE_ITEM_USAGE_TYPE like '%.24xl%'  then 192
                            when t.LINE_ITEM_USAGE_TYPE like '%.32xl%'  then 256
                            when t.LINE_ITEM_USAGE_TYPE like '%.12xl%' then 96
                            when t.LINE_ITEM_USAGE_TYPE like '%.10xl%'  then 80
                            when t.LINE_ITEM_USAGE_TYPE like '%.9xl%' then 72
                            when t.LINE_ITEM_USAGE_TYPE like '%.8xl%' then 64
                            when t.LINE_ITEM_USAGE_TYPE like '%.6xl%' then 48
                            when t.LINE_ITEM_USAGE_TYPE like '%.4xl%' then 32
                            when t.LINE_ITEM_USAGE_TYPE like '%.3xl%' then 24
                            when t.LINE_ITEM_USAGE_TYPE like '%.2xl%' then 16
                            when t.LINE_ITEM_USAGE_TYPE like '%.xl%' then 8
                            when t.LINE_ITEM_USAGE_TYPE like '%.l%'  then 4
                            else line_item_normalization_factor
                            end as nfactor_used,
       case
	    WHEN line_item_operation like 'RunInstances:0004%'
		THEN 'Linux/UNIX and SQL Server Standard'
		WHEN line_item_operation like 'RunInstances:0100%'
		THEN 'Linux/UNIX and SQL Server Enterprise'
		WHEN line_item_operation like 'RunInstances:0200%'
		THEN 'Linux/UNIX and SQL Server Web'
		WHEN line_item_operation like 'RunInstances:0010%'
		THEN 'Red Hat Enterprise Linux'
		WHEN line_item_operation like 'RunInstances:1010%'
		THEN 'Red Hat Enterprise Linux with HA'
		WHEN line_item_operation like 'RunInstances:000g%'
		THEN 'SUSE'
		WHEN line_item_operation like 'RunInstances:0002%'
		THEN 'Windows'
		WHEN line_item_operation like 'RunInstances:0006%'
		THEN 'Windows and SQL Server Standard'
		WHEN line_item_operation like 'RunInstances:0102%'
		THEN 'Windows and SQL Server Enterprise'
		WHEN line_item_operation like 'RunInstances:0202%'
		THEN 'Windows and SQL Server Web'
		WHEN line_item_operation like 'RunInstances:0800%'
		THEN 'Windows (BYOL)'
		WHEN line_item_operation like 'RunInstances'
		THEN 'Linux/UNIX'
		when line_item_operation like 'RunInstances:0014%'
		then 'Red Hat Enterprise Linux and SQL Server Standard'
		when line_item_operation like 'RunInstances:0110%'
		then 'Red Hat Enterprise Linux and SQL Server Enterprise'
		when line_item_operation like 'RunInstances:0210%'
		then 'Red Hat Enterprise Linux and SQL Server Web'
		when line_item_operation like 'RunInstances:1014%'
		then 'Red Hat Enterprise Linux with HA and SQL Server Standard'
		when line_item_operation like 'RunInstances:1110%'
		then 'Red Hat Enterprise Linux with HA and SQL Server Enterprise'
		else ''
		end as mycloud_operatingsystem,
    case 
 when (instance_type ilike ('%g4ad%') or instance_type ilike ('%g5%') or instance_type ilike ('%g5g%') or
                                instance_type ilike ('%g6%') or instance_type ilike ('%g6e%') or instance_type ilike ('%gr6%') or instance_type ilike ('%inf1%') or instance_type ilike ('%g4dn%') or instance_type ilike ('%inf2%') or
                                instance_type ilike ('%hpc7a%') or instance_type ilike ('%p5%') or instance_type ilike ('%u7i-6tb%')
                                or instance_type ilike ('%u7i-8tb%'))
                                then instance_type
                                when MYCLOUD_OPERATINGSYSTEM like '%BYOL%'
                                then SPLIT_PART(instance_type, '.', 1)
                                when MYCLOUD_OPERATINGSYSTEM = 'Linux/UNIX'
                                then SPLIT_PART(instance_type, '.', 1)
                                else instance_type
                                end as MYCLOUD_FAMILY_FLEXIBLE,
    MAX(CASE WHEN f.value:key = 'region' THEN f.value:value::STRING END) AS region_code,
    MAX(CASE WHEN f.value:key = 'product_name' THEN f.value:value::STRING END) AS product_productname,
      (case 
                WHEN region_code like 'us-east-2%' OR LINE_ITEM_USAGE_TYPE like '%USE2-%' 
                THEN 'Ohio'
                WHEN region_code like 'us-east-1%'OR LINE_ITEM_USAGE_TYPE like '%USE1-%'  
                THEN 'N. Virginia'
                WHEN region_code like 'us-west-1%' OR LINE_ITEM_USAGE_TYPE like '%USW1-%' 
                THEN 'N. California'
                WHEN region_code like 'us-west-2%' OR LINE_ITEM_USAGE_TYPE like '%USW2-%' 
                THEN 'Oregon'
                WHEN region_code like 'ap-east-1%' OR LINE_ITEM_USAGE_TYPE like '%APE1-%' 
                THEN 'Hong Kong'
                WHEN region_code like 'ap-south-1%' OR LINE_ITEM_USAGE_TYPE like '%APS3-%' 
                THEN 'Mumbai'
                WHEN region_code like 'ap-northeast-3%' OR LINE_ITEM_USAGE_TYPE like '%APN3-%'
                THEN 'Osaka'
                WHEN region_code like 'ap-northeast-2%' OR LINE_ITEM_USAGE_TYPE like '%APN2-%' 
                THEN 'Seoul'
                WHEN region_code like 'ap-southeast-1%' OR LINE_ITEM_USAGE_TYPE like '%APS1-%' 
                THEN 'Singapore'
                WHEN region_code like 'ap-southeast-2%' OR LINE_ITEM_USAGE_TYPE like '%APS2-%'
                THEN 'Sydney'
                WHEN region_code like 'ap-southeast-3%' OR LINE_ITEM_USAGE_TYPE like '%APS4-%'
                THEN 'Jakarta'
                WHEN region_code like 'ap-northeast-1%' OR LINE_ITEM_USAGE_TYPE like '%APN1-%' 
                THEN 'Tokyo'
                WHEN region_code like 'ca-central-1%' OR LINE_ITEM_USAGE_TYPE like '%CAN1-%'  
                THEN 'Central'
                WHEN region_code like 'eu-west-1%' OR LINE_ITEM_USAGE_TYPE like '%EU-%'  
                THEN 'Ireland'
                WHEN region_code like 'eu-west-1%' OR LINE_ITEM_USAGE_TYPE like '%EUW1-%'
                THEN 'Ireland'
                WHEN region_code like 'eu-central-1%' OR LINE_ITEM_USAGE_TYPE like '%EUC1-%'
                THEN 'Frankfurt'
                WHEN region_code like 'eu-west-2%' OR LINE_ITEM_USAGE_TYPE like '%EUW2-%' 
                THEN 'London'
                WHEN region_code like 'eu-west-3%' OR LINE_ITEM_USAGE_TYPE like '%EUW3-%'
                THEN 'Paris'
                WHEN region_code like 'eu-north-1%' OR LINE_ITEM_USAGE_TYPE like '%EUN1-%'  
                THEN 'Stockholm'
                WHEN region_code like 'me-south-1%' OR LINE_ITEM_USAGE_TYPE like '%MES1-%' 
                THEN 'Bahrain'
                WHEN region_code like 'sa-east-1%' OR LINE_ITEM_USAGE_TYPE like '%SAE1-%' 
                THEN 'Sao Paulo'
                WHEN region_code like 'ug-west-1%' OR LINE_ITEM_USAGE_TYPE like '%UGW1-%' 
                THEN 'GovCloud (US)'
                WHEN region_code like 'ug-east-1%' OR LINE_ITEM_USAGE_TYPE like '%UGE1-%' 
                THEN 'GovCloud (US-East)'
                WHEN region_code like 'af-south-1%' OR LINE_ITEM_USAGE_TYPE like '%AFS1-%'       
                THEN 'Cape Town'
                WHEN region_code like 'eu-south-1%' or LINE_ITEM_USAGE_TYPE like '%EUS1-%' 
                THEN 'Milan'
                WHEN region_code like 'me-central-1%' or LINE_ITEM_USAGE_TYPE like '%MEC1-%'
                THEN 'UAE'
                WHEN region_code like 'ap-south-2%' or LINE_ITEM_USAGE_TYPE like '%APS2-%'
                THEN 'Hyderabad'
                else ''
        end) as mycloud_region,
    t.bill_payer_account_id,
    t.line_item_usage_account_id,
FROM analytics_application_table_#startyear_#startmonth t,
     LATERAL FLATTEN(input => t.product:key_value) f
WHERE t.line_item_line_item_type IN ('RIFee')
AND extract(month from t.line_item_usage_start_date) = #startmonth
and extract(year from t.line_item_usage_start_date) = #startyear
and bill_payer_account_id in (#payers_ids)

GROUP BY 
    t.reservation_reservation_a_r_n,
    t.LINE_ITEM_USAGE_TYPE,
    nfactor_used,
    t.line_item_operation,
    t.bill_payer_account_id,
    t.line_item_usage_account_id
HAVING MAX(CASE WHEN f.value:key = 'product_name' THEN f.value:value::STRING END) = 'Amazon Elastic Compute Cloud'

)

SELECT
    SPLIT_PART(m.RESERVATION_RESERVATION_A_R_N, '/', 2) AS reservation_id,
    line_item_usage_start_date as lineitem_usagestartdate,
	case when line_item_line_item_type = 'RIFee' then 
	DATEADD(SECOND, 1, line_item_usage_end_date) 
	else  line_item_usage_end_date end as lineitem_usageenddate,
    m.bill_payer_account_id as bill_payeraccountid,
	b.payer_name,
    r.line_item_usage_account_id as lineitem_usageaccountid,
    r.mycloud_family_flexible AS instancetype_family,
    r.INSTANCE_TYPE as instancetype,
    extract(month from line_item_usage_start_date) as mycloud_startmonth,
    extract(year from line_item_usage_start_date) as mycloud_startyear,
    r.product_productname,
    r.mycloud_operatingsystem,
    r.mycloud_region,
    pricing_offering_class AS offering_type,
    line_item_line_item_type as lineitem_lineitemtype,
    m.RESERVATION_RESERVATION_A_R_N as RESERVATION_RESERVATIONARN,
    reservation_number_of_reservations as reservation_numberofreservations,
SUM(
    CASE 
        WHEN line_item_line_item_type = 'RIFee' THEN 
            CASE 
                WHEN line_item_normalization_factor = 0 or line_item_normalization_factor is null  THEN r.nfactor_used
                ELSE line_item_normalization_factor
            END
        ELSE 0
    END
) AS lineitem_normalizationfactor,
    SUM( case 
            when line_item_normalized_usage_amount = 0 or line_item_normalized_usage_amount  is null
            then r.nfactor_used * line_item_usage_amount
            else line_item_normalized_usage_amount
            end
        ) AS lineitem_normalizedusageamount,
    SUM(line_item_unblended_cost) AS lineitem_unblendedcost,
    SUM(reservation_effective_cost) AS reservation_EffectiveCost,
    SUM(
        CASE
            WHEN pricing_public_on_demand_cost = 0 THEN 0
            ELSE pricing_public_on_demand_cost::DECIMAL(30,8)
        END
    ) AS mycloud_ondemandprice,
    SUM(
        CASE
            WHEN line_item_line_item_type = 'RIFee' THEN (
                COALESCE(line_item_unblended_rate::REAL, 0) +
                CASE
                    WHEN DATEDIFF(hour, line_item_usage_start_date, DATEADD(second, 2, line_item_usage_end_date)) = 0 THEN 0
                    ELSE (
                        reservation_amortized_upfront_fee_for_billing_period /
                        (
                            DATEDIFF(hour, line_item_usage_start_date, DATEADD(second, 2, line_item_usage_end_date)) * reservation_number_of_reservations
                        )
                    )
                END
            )
            ELSE 0
        END
    ) AS ri_hourly_effective_cost
FROM
    analytics_application_table_#startyear_#startmonth  m
      
    

LEFT JOIN
    payer_billdesk_config.payers b ON b.payer_account_id = m.bill_payer_account_id

left JOIN
    rifee_data r ON 
        r.RESERVATION_RESERVATION_A_R_N = m.RESERVATION_RESERVATION_A_R_N AND
        r.bill_payer_account_id = m.bill_payer_account_id 
      
WHERE
    extract(month from m.line_item_usage_start_date) = #startmonth
   and extract(year from m.line_item_usage_start_date) = #startyear 
   and m.line_item_line_item_type IN ('RIFee', 'DiscountedUsage', 'VDiscountedUsage')
    AND m.line_item_line_item_type NOT IN ('Credit', 'Tax', 'Refund')
    and m.bill_payer_account_id in (#payers_ids)
GROUP BY ALL;


CREATE OR REPLACE TEMPORARY TABLE ec2_ri_utilization_report_hourly_temp AS
WITH RECURSIVE hourSequence AS (
  SELECT 0 AS seq
  UNION ALL
  SELECT seq + 1
  FROM hourSequence
  WHERE seq < 24*DATEDIFF(DAY, date_from_parts(#startyear,#startmonth,1), dateadd('day',1,last_day(date_from_parts(#startyear,#startmonth,1)))) - 1
)
select
    RESERVATION_ID,
    bill_payeraccountid,
    payer_name,
    a.family_type ,
    a.customer_name ,
	lineitem_usageaccountid as linkedaccountid,
	DATEADD(hour, seq,date_from_parts(#startyear,#startmonth, 1)) AS startdate,
    DATEADD(second, -1, DATEADD(hour, 1, DATEADD(hour, seq, date_from_parts(#startyear,#startmonth,1)))) AS enddate,
	reservation_reservationarn as reservation_reservationarn,
	mycloud_startmonth,
	mycloud_startyear,
	mycloud_region as region,
    offering_type,
	mycloud_operatingsystem as operatingSystem,
    INSTANCETYPE_FAMILY,
	instancetype,
    sum(lineitem_normalizationfactor) as NFapplied,
	coalesce(sum(mycloud_ondemandprice), 0) as onDemandCost,
	sum(case when lineitem_lineitemtype in('RIFee') then reservation_numberofreservations*(DATEDIFF(hour, greatest(lineitem_usagestartdate, cast(startDate as timestamp) ), dateadd(second, 2, least(lineitem_usageenddate, cast(endDate as timestamp))) )) else 0 end) as reservedHours,
    sum(case when lineitem_lineitemtype in ('RIFee') then reservation_numberofreservations*lineitem_normalizationfactor else 0 end) as total_NU,
	sum(case when lineitem_lineitemtype in ('RIFee') then reservation_numberofreservations else 0 end) as quantity,
    sum(case when lineitem_lineitemtype in ('DiscountedUsage', 'VDiscountedUsage')
		then lineitem_normalizedusageamount
	    else 0 end) as Used_NU,
	sum(case when lineitem_lineitemtype in ('DiscountedUsage', 'VDiscountedUsage')
		then mycloud_ondemandprice
	    else 0 end) as od_cost,
	sum(case when lineitem_lineitemtype in ('RIFee')
		then ri_hourly_effective_cost
		else 0 end) as ri_ecost,
	sum(case when lineitem_lineitemtype in ('DiscountedUsage', 'VDiscountedUsage')
		then mycloud_ondemandprice
	    else 0 end) - sum(case when lineitem_lineitemtype in ('RIFee')
		then ri_hourly_effective_cost
		else 0 end)*reservedHours as netSavings,
    sum(case when(lineitem_lineitemtype in('DiscountedUsage', 'VDiscountedUsage') and lineitem_usagestartdate>= cast(startDate as timestamp) and lineitem_usagestartdate<= cast(endDate as timestamp)) then (lineitem_normalizedusageamount) else 0 end) as usedHours
from
	ec2_ri_report_hourly_temp a2
cross join hourSequence wtp

    join (select * from payer_billdesk_config.account) a on split_part(a2.reservation_reservationarn, ':', 5) =a.account_id

where
		mycloud_startmonth= #startmonth
	AND mycloud_startyear= #startyear  and
  	((( cast(startDate as timestamp) between lineitem_usagestartdate and lineitem_usageenddate
	or cast(endDate as timestamp) between lineitem_usagestartdate and lineitem_usageenddate
 or
				            lineitem_usagestartdate
					between
					cast(startDate as timestamp)
        					and
        					cast(endDate as timestamp))
	and datediff(hour,cast(startdate as timestamp),lineitem_usageenddate ) != 0
	and lineitem_lineitemtype in ('RIFee'))
	or (lineitem_lineitemtype in ('DiscountedUsage', 'VDiscountedUsage')
	and lineitem_usagestartdate>= cast(startDate as timestamp)
	and lineitem_usagestartdate<= cast(endDate as timestamp)))
	and product_productname = 'Amazon Elastic Compute Cloud'
	and reservation_reservationarn != ''
	and lineitem_lineitemtype in ('RIFee', 'DiscountedUsage', 'VDiscountedUsage')
	group by all;



DELETE
FROM ck_analytics_application_ri_wastage_hourly
WHERE
extract(month from lineitem_usagestartdate) = #startmonth
and extract(year from lineitem_usagestartdate) = #startyear
and bill_payeraccountid in (#payers_ids)
and product_name = 'EC2(RIs)';


insert into ck_analytics_application_ri_wastage_hourly (
RESERVATION_ARN,PAYER_NAME,BILL_PAYERACCOUNTID,USAGEACCOUNTID,ACCOUNT_TYPE,CUSTOMER_NAME,REGION,PRODUCT_NAME,
OPERATINGSYSTEM_ENGINE,INSTANCETYPE_FAMILY,SAVINGSPLAN_TYPE,LINEITEM_USAGESTARTDATE,TOTAL_NU,USED_NU,UNUSED_NU,
WASTAGE_PERCENTAGE,OFFERING_TYPE,RESERVATION_RESERVATIONARN,RESERVEDHOURS,USED_HOURS,UNUSED_HOURS,WASTAGE_COST,
RI_UTILIZATION,MYCLOUD_STARTMONTH,MYCLOUD_STARTYEAR,INSTANCETYPE,NFAPPLIED,QUANTITY,RI_ECOST
)
select
RESERVATION_ID ,
PAYER_NAME,
a.BILL_PAYERACCOUNTID,
LINKEDACCOUNTID,
FAMILY_TYPE,
CUSTOMER_NAME,
REGION,
'EC2(RIs)' as product_name,
OPERATINGSYSTEM,
INSTANCETYPE_FAMILY,
'' as SAVINGSPLAN_TYPE,
STARTDATE,
TOTAL_NU,
USED_NU,
total_nu - used_nu as unused_nu,
unused_nu/total_nu as wastage_percentage,
OFFERING_TYPE,
RESERVATION_RESERVATIONARN,
RESERVEDHOURS,
USEDHOURS/NFAPPLIED as actual_used_hours,
reservedHours - actual_used_hours as unused_hours,
(reservedHours - actual_used_hours)*ri_ecost as wastage_cost,
(actual_used_hours/reservedHours)*1 as ri_utilization,
MYCLOUD_STARTMONTH,
MYCLOUD_STARTYEAR,
INSTANCETYPE,
NFAPPLIED,
QUANTITY,
RI_ECOST
from ec2_ri_utilization_report_hourly_temp a
left join ec2_payer_max_date b on a.BILL_PAYERACCOUNTID = b.BILL_PAYERACCOUNTID 
where 
a.STARTDATE <= b.max_date and 
reservedHours <> 0;





-------------------- RDS Wastage ------------------------------------------

CREATE OR REPLACE TEMPORARY TABLE rds_payer_max_date as
select
bill_payer_account_id as bill_payeraccountid,
DATEADD(SECOND, -1, max(line_item_usage_start_date)) AS MAX_DATE
from analytics_application_table_#startyear_#startmonth t,
      LATERAL FLATTEN(input => t.product:key_value) f
WHERE
t.line_item_line_item_type = 'DiscountedUsage' 
and f.value:key in ('product_name')
AND f.value:value = 'Amazon Relational Database Service'
and bill_payer_account_id in (#payers_ids)
group by 1;




CREATE OR replace TEMPORARY TABLE rds_ri_util_report_temp AS 
WITH rifee_data AS (
    SELECT
        distinct
        t.reservation_reservation_a_r_n,
        SPLIT_PART(t.LINE_ITEM_USAGE_TYPE, ':', 2) AS instance_type,
        case
        WHEN line_item_usage_type like '%.nano%'  and (line_item_usage_type like '%InstanceUsage%' or line_item_usage_type like '%HeavyUsage%')
                            THEN 0.25
                            WHEN line_item_usage_type like '%.nano%' and (line_item_usage_type like '%Multi-AZ%' or line_item_usage_type like '%MirrorUsage%')
                            THEN 0.5
                            WHEN line_item_usage_type like '%.micro%'  and (line_item_usage_type like '%InstanceUsage%' or line_item_usage_type like '%HeavyUsage%')
                            THEN 0.5
                            WHEN line_item_usage_type like '%.micro%'  and (line_item_usage_type like '%Multi-AZ%' or line_item_usage_type like '%MirrorUsage%')
                            THEN 1
                            WHEN line_item_usage_type like '%.small%'  and (line_item_usage_type like '%InstanceUsage%' or line_item_usage_type like '%HeavyUsage%')
                            THEN 1
                            WHEN line_item_usage_type like '%.small%'  and (line_item_usage_type like '%Multi-AZ%' or line_item_usage_type like '%MirrorUsage%')
                            THEN 2
                            WHEN line_item_usage_type like '%.medium%'  and (line_item_usage_type like '%InstanceUsage%' or line_item_usage_type like '%HeavyUsage%')
                            THEN 2
                            WHEN line_item_usage_type like '%.medium%'  and (line_item_usage_type like '%Multi-AZ%' or line_item_usage_type like '%MirrorUsage%')
                            THEN 4
                            WHEN line_item_usage_type like '%.16xl%'  and (line_item_usage_type like '%InstanceUsage%' or line_item_usage_type like '%HeavyUsage%')
                            THEN 128
                            WHEN line_item_usage_type like '%.16xl%'  and (line_item_usage_type like '%Multi-AZ%' or line_item_usage_type like '%MirrorUsage%')
                            THEN 256
                            WHEN line_item_usage_type like '%.18xl%'  and (line_item_usage_type like '%InstanceUsage%' or line_item_usage_type like '%HeavyUsage%')
                            THEN 144
                            WHEN line_item_usage_type like '%.18xl%'   and (line_item_usage_type like '%Multi-AZ%' or line_item_usage_type like '%MirrorUsage%')
                            THEN 288
                            WHEN line_item_usage_type like '%.24xl%'  and (line_item_usage_type like '%InstanceUsage%' or line_item_usage_type like '%HeavyUsage%')
                            THEN 192
                            WHEN line_item_usage_type like '%.24xl%'  and (line_item_usage_type like '%Multi-AZ%' or line_item_usage_type like '%MirrorUsage%')
                            THEN 384
                            WHEN line_item_usage_type like '%.32xl%'  and (line_item_usage_type like '%InstanceUsage%' or line_item_usage_type like '%HeavyUsage%')
                            THEN 256
                            WHEN line_item_usage_type like '%.32xl%'  and (line_item_usage_type like '%Multi-AZ%' or line_item_usage_type like '%MirrorUsage%')
                            THEN 512
                            WHEN line_item_usage_type like '%.12xl%'  and (line_item_usage_type like '%InstanceUsage%' or line_item_usage_type like '%HeavyUsage%')
                            THEN 96
                            WHEN line_item_usage_type like '%.12xl%'  and (line_item_usage_type like '%Multi-AZ%' or line_item_usage_type like '%MirrorUsage%')
                            THEN 192
                            WHEN line_item_usage_type like '%.10xl%'  and (line_item_usage_type like '%InstanceUsage%' or line_item_usage_type like '%HeavyUsage%')
                            THEN 80
                            WHEN line_item_usage_type like '%.10xl%'  and (line_item_usage_type like '%Multi-AZ%' or line_item_usage_type like '%MirrorUsage%')
                            THEN 160
                            WHEN line_item_usage_type like '%.9xl%'   and (line_item_usage_type like '%InstanceUsage%' or line_item_usage_type like '%HeavyUsage%')
                            THEN 72
                            WHEN line_item_usage_type like '%.9xl%'   and (line_item_usage_type like '%Multi-AZ%' or line_item_usage_type like '%MirrorUsage%')
                            THEN 144
                            WHEN line_item_usage_type like '%.8xl%'  and (line_item_usage_type like '%InstanceUsage%' or line_item_usage_type like '%HeavyUsage%')
                            THEN 64
                            WHEN line_item_usage_type like '%.8xl%'  and (line_item_usage_type like '%Multi-AZ%' or line_item_usage_type like '%MirrorUsage%')
                            THEN 128
                            WHEN line_item_usage_type like '%.6xl%'  and (line_item_usage_type like '%InstanceUsage%' or line_item_usage_type like '%HeavyUsage%')
                            THEN 48
                            WHEN line_item_usage_type like '%.6xl%'  and (line_item_usage_type like '%Multi-AZ%' or line_item_usage_type like '%MirrorUsage%')
                            THEN 96
                            WHEN line_item_usage_type like '%.4xl%'   and (line_item_usage_type like '%InstanceUsage%' or line_item_usage_type like '%HeavyUsage%')
                            THEN 32
                            WHEN line_item_usage_type like '%.4xl%' and (line_item_usage_type like '%Multi-AZ%' or line_item_usage_type like '%MirrorUsage%')
                            THEN 64
                            WHEN line_item_usage_type like '%.3xl%'  and (line_item_usage_type like '%InstanceUsage%' or line_item_usage_type like '%HeavyUsage%')
                            THEN 24
                            WHEN line_item_usage_type like '%.3xl%'   and (line_item_usage_type like '%Multi-AZ%' or line_item_usage_type like '%MirrorUsage%')
                            THEN 48
                            WHEN line_item_usage_type like '%.2xl%'   and (line_item_usage_type like '%InstanceUsage%' or line_item_usage_type like '%HeavyUsage%')
                            THEN 16
                            WHEN line_item_usage_type like '%.2xl%'  and (line_item_usage_type like '%Multi-AZ%' or line_item_usage_type like '%MirrorUsage%')
                            THEN 32
                            WHEN line_item_usage_type like '%.xl%'   and (line_item_usage_type like '%InstanceUsage%' or line_item_usage_type like '%HeavyUsage%')
                            THEN 8
                            WHEN line_item_usage_type like '%.xl%'  and (line_item_usage_type like '%Multi-AZ%' or line_item_usage_type like '%MirrorUsage%')
                            THEN 16
                            WHEN line_item_usage_type like '%.l%'  and (line_item_usage_type like '%InstanceUsage%' or line_item_usage_type like '%HeavyUsage%')
                            THEN 4
                            WHEN line_item_usage_type like '%.l%'  and (line_item_usage_type like '%Multi-AZ%' or line_item_usage_type like '%MirrorUsage%')
                            THEN 8
                            else line_item_normalization_factor
                            end as nfactor_used,
        case
		WHEN line_item_operation = 'CreateDBInstance:0002'
		THEN 'MySQL'
		WHEN line_item_operation = 'CreateDBInstance:0003'
		THEN 'Oracle SE1 (BYOL)'
		WHEN line_item_operation = 'CreateDBInstance:0004'
		THEN 'Oracle SE (BYOL)'
		WHEN line_item_operation = 'CreateDBInstance:0005'
		THEN 'Oracle EE (BYOL)'
		WHEN line_item_operation = 'CreateDBInstance:0006'
		THEN 'Oracle SE1 (LI)'
		WHEN line_item_operation = 'CreateDBInstance:0008'
		THEN 'SQL Server SE (BYOL)'
		WHEN line_item_operation = 'CreateDBInstance:0009'
		THEN 'SQL Server EE (BYOL)'
		WHEN line_item_operation = 'CreateDBInstance:0010'
		THEN 'SQL Server Exp (LI)'
		WHEN line_item_operation = 'CreateDBInstance:0011'
		THEN 'SQL Server Web (LI)'
		WHEN line_item_operation = 'CreateDBInstance:0012'
		THEN 'SQL Server SE (LI)'
		WHEN line_item_operation = 'CreateDBInstance:0014'
		THEN 'PostgreSQL'
		WHEN line_item_operation = 'CreateDBInstance:0015'
		THEN 'SQL Server EE (LI)'
		WHEN line_item_operation = 'CreateDBInstance:0016'
		THEN 'Aurora MySQL'
		WHEN line_item_operation = 'CreateDBInstance:0018'
		THEN 'MariaDB'
		WHEN line_item_operation = 'CreateDBInstance:0019'
		THEN 'Oracle SE2 (BYOL)'
		WHEN line_item_operation = 'CreateDBInstance:0020'
		THEN 'Oracle SE2 (LI)'
		WHEN line_item_operation = 'CreateDBInstance:0021'
		THEN 'Aurora PostgreSQL'
		WHEN line_item_operation = 'CreateDBInstance:0022'
		THEN 'Neptune'
		WHEN line_item_operation = 'CreateDBInstance:0232'
		THEN 'SQL Server (on-premise for Outpost) Web'
		WHEN line_item_operation = 'CreateDBInstance:0230'
		THEN 'SQL Server (on-premise for Outpost) EE'
		WHEN line_item_operation = 'CreateDBInstance:0231'
		THEN 'SQL Server (on-premise for Outpost) SE'
		WHEN line_item_operation = 'CreateDBInstance:0028'
		THEN 'Db2 SE (BYOL)'
		WHEN line_item_operation = 'CreateDBInstance:0029'
		THEN 'Db2 AE (BYOL)'
		WHEN line_item_operation = 'CreateDBInstance:0034'
		THEN 'Db2 SE (AWS Marketplace)'
		WHEN line_item_operation= 'CreateDBInstance:0035'
		THEN 'Db2 AE (AWS Marketplace)'
		else ''
		end as mycloud_operatingsystem,
    case 
 when (instance_type ilike ('%g4ad%') or instance_type ilike ('%g5%') or instance_type ilike ('%g5g%') or
                                instance_type ilike ('%g6%') or instance_type ilike ('%g6e%') or instance_type ilike ('%gr6%') or instance_type ilike ('%inf1%') or instance_type ilike ('%g4dn%') or instance_type ilike ('%inf2%') or
                                instance_type ilike ('%hpc7a%') or instance_type ilike ('%p5%') or instance_type ilike ('%u7i-6tb%')
                                or instance_type ilike ('%u7i-8tb%'))
                                then instance_type
                                when MYCLOUD_OPERATINGSYSTEM  like '%BYOL%'
                                then SPLIT_PART(instance_type, '.', 2)
                                when MYCLOUD_OPERATINGSYSTEM  in ('MySQL', 'Aurora MySQL', 'PostgreSQL', 'Aurora PostgreSQL', 'MariaDB')
                                then SPLIT_PART(instance_type, '.', 2)
                                else instance_type
                                end as mycloud_family_flexible,
        MAX(CASE WHEN f.value:key = 'region' THEN f.value:value::STRING END) AS region_code,
        MAX(CASE WHEN f.value:key = 'product_name' THEN f.value:value::STRING END) AS product_productname,
    case
	WHEN region_code like 'us-east-2%' OR LINE_ITEM_USAGE_TYPE like '%USE2-%' 
	THEN 'Ohio'
	WHEN region_code like 'us-east-1%'OR LINE_ITEM_USAGE_TYPE like '%USE1-%' 
	THEN 'N. Virginia'
	WHEN region_code like 'us-west-1%' OR LINE_ITEM_USAGE_TYPE like '%USW1-%' 
	THEN 'N. California'
	WHEN region_code like 'us-west-2%' OR LINE_ITEM_USAGE_TYPE like '%USW2-%' 
	THEN 'Oregon'
	WHEN region_code like 'ap-east-1%' OR LINE_ITEM_USAGE_TYPE like '%APE1-%' 
	THEN 'Hong Kong'
	WHEN region_code like 'ap-south-1%' OR LINE_ITEM_USAGE_TYPE like '%APS3-%' 
	THEN 'Mumbai'
	WHEN region_code like 'ap-northeast-3%' OR LINE_ITEM_USAGE_TYPE like '%APN3-%' 
	THEN 'Osaka'
	WHEN region_code like 'ap-northeast-2%' OR LINE_ITEM_USAGE_TYPE like '%APN2-%' 
	THEN 'Seoul'
	WHEN region_code like 'ap-southeast-1%' OR LINE_ITEM_USAGE_TYPE like '%APS1-%' 
	THEN 'Singapore'
	WHEN region_code like 'ap-southeast-2%' OR LINE_ITEM_USAGE_TYPE like '%APS2-%' 
	THEN 'Sydney'
	WHEN region_code like 'ap-southeast-3%' OR LINE_ITEM_USAGE_TYPE like '%APS4-%' 
	THEN 'Jakarta'
	WHEN region_code like 'ap-northeast-1%' OR LINE_ITEM_USAGE_TYPE like '%APN1-%' 
	THEN 'Tokyo'
	WHEN region_code like 'ca-central-1%' OR LINE_ITEM_USAGE_TYPE like '%CAN1-%' 
	THEN 'Central'
	WHEN region_code like 'eu-west-1%' OR LINE_ITEM_USAGE_TYPE like '%EU-%' 
	THEN 'Ireland'
	WHEN region_code like 'eu-west-1%' OR LINE_ITEM_USAGE_TYPE like '%EUW1-%' 
	THEN 'Ireland'
	WHEN region_code like 'eu-central-1%' OR LINE_ITEM_USAGE_TYPE like '%EUC1-%' 
	THEN 'Frankfurt'
	WHEN region_code like 'eu-west-2%' OR LINE_ITEM_USAGE_TYPE like '%EUW2-%' 
	THEN 'London'
	WHEN region_code like 'eu-west-3%' OR LINE_ITEM_USAGE_TYPE like '%EUW3-%' 
	THEN 'Paris'
	WHEN region_code like 'eu-north-1%' OR LINE_ITEM_USAGE_TYPE like '%EUN1-%' 
	THEN 'Stockholm'
	WHEN region_code like 'me-south-1%' OR LINE_ITEM_USAGE_TYPE like '%MES1-%' 
	THEN 'Bahrain'
	WHEN region_code like 'sa-east-1%' OR LINE_ITEM_USAGE_TYPE like '%SAE1-%' 
	THEN 'Sao Paulo'
	WHEN region_code like 'ug-west-1%' OR LINE_ITEM_USAGE_TYPE like '%UGW1-%' 
	THEN 'GovCloud (US)'
	WHEN region_code like 'ug-east-1%' OR LINE_ITEM_USAGE_TYPE like '%UGE1-%' 
	THEN 'GovCloud (US-East)'
	WHEN region_code like 'af-south-1%' OR LINE_ITEM_USAGE_TYPE like '%AFS1-%' 
	THEN 'Cape Town'
	WHEN region_code like 'eu-south-1%' or LINE_ITEM_USAGE_TYPE like '%EUS1-%' 
    THEN 'Milan'
    WHEN region_code like 'me-central-1%' or LINE_ITEM_USAGE_TYPE like '%MEC1-%' 
    THEN 'UAE'
    WHEN region_code like 'ap-south-2%' or LINE_ITEM_USAGE_TYPE like '%APS2-%' 
    THEN 'Hyderabad'
	else ''
	end as mycloud_region,
    t.bill_payer_account_id,
    t.line_item_usage_account_id
    FROM
        analytics_application_table_#startyear_#startmonth t,
     LATERAL FLATTEN(input => t.product:key_value) f
WHERE t.line_item_line_item_type IN ('RIFee')
AND extract(month from t.line_item_usage_start_date) = #startmonth
and extract(year from t.line_item_usage_start_date) = #startyear
and bill_payer_account_id in (#payers_ids)
GROUP BY 
    t.reservation_reservation_a_r_n,
    t.LINE_ITEM_USAGE_TYPE,
    nfactor_used,
    t.line_item_operation,
    t.bill_payer_account_id,
    t.line_item_usage_account_id
HAVING MAX(CASE WHEN f.value:key = 'product_name' THEN f.value:value::STRING END) = 'Amazon Relational Database Service'
)
	select
        split_part(m.RESERVATION_RESERVATION_A_R_N, ':', 7) as reservation_id,
    line_item_usage_start_date as lineitem_usagestartdate,
        case when line_item_line_item_type = 'RIFee' then 
	    DATEADD(SECOND, 1, line_item_usage_end_date) 
	    else  line_item_usage_end_date end as lineitem_usageenddate,
    m.bill_payer_account_id as bill_payeraccountid,
        
	    b.payer_name,
        
    r.line_item_usage_account_id as lineitem_usageaccountid,
    r.mycloud_family_flexible AS instancetype_family,
    r.INSTANCE_TYPE as instancetype,
    extract(month from line_item_usage_start_date) as mycloud_startmonth,
    extract(year from line_item_usage_start_date) as mycloud_startyear,
    r.product_productname,
		r.mycloud_operatingsystem,
		r.mycloud_region,
    pricing_offering_class AS offering_type,
    line_item_line_item_type as lineitem_lineitemtype,
    m.RESERVATION_RESERVATION_A_R_N as RESERVATION_RESERVATIONARN,
    reservation_number_of_reservations as reservation_numberofreservations,
        
	SUM(
    CASE 
        WHEN line_item_line_item_type = 'RIFee' THEN 
            CASE 
                WHEN line_item_normalization_factor = 0 or line_item_normalization_factor is null  THEN r.nfactor_used
                ELSE line_item_normalization_factor
            END
        ELSE 0
    END
) AS lineitem_normalizationfactor,
    SUM( case 
            when line_item_normalized_usage_amount = 0 or line_item_normalized_usage_amount  is null
            then r.nfactor_used * line_item_usage_amount
            else line_item_normalized_usage_amount
            end
        ) AS lineitem_normalizedusageamount,
		sum(line_item_unblended_cost) as lineitem_unblendedcost,
		sum(reservation_effective_cost) as reservation_EffectiveCost,
		SUM(
        CASE
            WHEN pricing_public_on_demand_cost = 0 THEN 0
            ELSE pricing_public_on_demand_cost::DECIMAL(30,8)
        END
    ) AS mycloud_ondemandprice,
    SUM(
        CASE
            WHEN line_item_line_item_type = 'RIFee' THEN (
                COALESCE(line_item_unblended_rate::REAL, 0) +
                CASE
                    WHEN DATEDIFF(hour, line_item_usage_start_date, DATEADD(second, 2, line_item_usage_end_date)) = 0 THEN 0
                    ELSE (
                        reservation_amortized_upfront_fee_for_billing_period /
                        (
                            DATEDIFF(hour, line_item_usage_start_date, DATEADD(second, 2, line_item_usage_end_date)) * reservation_number_of_reservations
                        )
                    )
                END
            )
            ELSE 0
        END
    ) AS ri_hourly_effective_cost		 
	from analytics_application_table_#startyear_#startmonth m

LEFT JOIN
    payer_billdesk_config.payers b ON b.payer_account_id = m.bill_payer_account_id
   
 LEFT JOIN
    rifee_data r ON 
        r.RESERVATION_RESERVATION_A_R_N = m.RESERVATION_RESERVATION_A_R_N AND
        r.bill_payer_account_id = m.bill_payer_account_id 
WHERE
 
    extract(month from m.line_item_usage_start_date) = #startmonth
   and extract(year from m.line_item_usage_start_date) = #startyear 
    and m.bill_payer_account_id in (#payers_ids) and
		 line_item_line_item_type not in ('Credit','Tax','Refund')
		and line_item_line_item_type  in ('DiscountedUsage','VDiscountedUsage','RIFee')
	group by all;


CREATE OR REPLACE TEMPORARY TABLE rds_ri_utilization_report_hourly_temp AS 
 WITH RECURSIVE hourSequence AS (
  SELECT 0 AS seq
  UNION ALL
  SELECT seq + 1
  FROM hourSequence
WHERE seq < 24*DATEDIFF(DAY, date_from_parts(#startyear, #startmonth, 1),dateadd('day',1,last_day(date_from_parts(#startyear, #startmonth,1)))) - 1)
select
    RESERVATION_ID,
    bill_payeraccountid,
    
    payer_name,
    a.family_type ,
    a.customer_name ,
    
	lineitem_usageaccountid as linkedaccountid,
    DATEADD(hour, seq,date_from_parts(#startyear,#startmonth, 1)) AS startdate,
    DATEADD(second, -1, DATEADD(hour, 1, DATEADD(hour, seq, date_from_parts(#startyear,#startmonth, 1)))) AS enddate,
    reservation_reservationarn as reservation_reservationarn,
	mycloud_startmonth,
	mycloud_startyear,
	mycloud_region as region,
    offering_type,
	mycloud_operatingsystem as operatingSystem,
    INSTANCETYPE_FAMILY,
	instancetype,
    sum(lineitem_normalizationfactor) as NFapplied,
	coalesce(sum(mycloud_ondemandprice), 0) as onDemandCost,
    sum(case when lineitem_lineitemtype in('RIFee') then reservation_numberofreservations*(DATEDIFF(hour, greatest(lineitem_usagestartdate, cast(startDate as timestamp) ), dateadd(second, 2, least(lineitem_usageenddate, cast(endDate as timestamp))) )) else 0 end) as reservedHours,
       sum(case when lineitem_lineitemtype in ('RIFee') then reservation_numberofreservations*lineitem_normalizationfactor else 0 end) as total_NU,
	sum(case when lineitem_lineitemtype in ('RIFee') then reservation_numberofreservations else 0 end) as quantity,
     sum(case when lineitem_lineitemtype in ('DiscountedUsage', 'VDiscountedUsage')
		then lineitem_normalizedusageamount
	    else 0 end) as Used_NU,
sum(case when lineitem_lineitemtype in ('DiscountedUsage', 'VDiscountedUsage')
		then mycloud_ondemandprice
	    else 0 end) as od_cost,
	sum(case when lineitem_lineitemtype in ('RIFee')
		then ri_hourly_effective_cost
		else 0 end) as ri_ecost,
	sum(case when lineitem_lineitemtype in ('DiscountedUsage', 'VDiscountedUsage')
		then mycloud_ondemandprice
	    else 0 end) - sum(case when lineitem_lineitemtype in ('RIFee')
		then ri_hourly_effective_cost
		else 0 end)*reservedHours as netSavings,
	sum(case when(lineitem_lineitemtype in('DiscountedUsage', 'VDiscountedUsage') and lineitem_usagestartdate>= cast(startDate as timestamp) and lineitem_usagestartdate<= cast(endDate as timestamp) ) then lineitem_normalizedusageamount else 0 end) as usedHours
from
	rds_ri_util_report_temp a2
	cross join hourSequence wtp

    join (select * from payer_billdesk_config.account) a on split_part(a2.reservation_reservationarn, ':', 5) =a.account_id

where 
		mycloud_startmonth= #startmonth
	AND mycloud_startyear= #startyear
	and ((( cast(startDate as timestamp) between lineitem_usagestartdate and lineitem_usageenddate
	or cast(endDate as timestamp) between lineitem_usagestartdate and lineitem_usageenddate
 or 
				            lineitem_usagestartdate 
					between 
					cast(startDate as timestamp)
        					and 
        					cast(endDate as timestamp))
	and datediff(hour,cast(startdate as timestamp),lineitem_usageenddate ) != 0
	and lineitem_lineitemtype in ('RIFee'))
	or (lineitem_lineitemtype in ('DiscountedUsage', 'VDiscountedUsage')
	and lineitem_usagestartdate>= cast(startDate as timestamp)
	and lineitem_usagestartdate<= cast(endDate as timestamp) ))
	and product_productname = 'Amazon Relational Database Service'
	and reservation_reservationarn != ''
	and lineitem_lineitemtype in ('RIFee', 'DiscountedUsage', 'VDiscountedUsage')	
group by all;



DELETE
FROM ck_analytics_application_ri_wastage_hourly
WHERE
extract(month from lineitem_usagestartdate) = #startmonth
and extract(year from lineitem_usagestartdate) = #startyear
 and bill_payeraccountid in (#payers_ids)
and product_name = 'RDS';


insert into ck_analytics_application_ri_wastage_hourly (
RESERVATION_ARN,PAYER_NAME,BILL_PAYERACCOUNTID,USAGEACCOUNTID,ACCOUNT_TYPE,CUSTOMER_NAME,REGION,PRODUCT_NAME,
OPERATINGSYSTEM_ENGINE,INSTANCETYPE_FAMILY,SAVINGSPLAN_TYPE,LINEITEM_USAGESTARTDATE,TOTAL_NU,USED_NU,UNUSED_NU,
WASTAGE_PERCENTAGE,OFFERING_TYPE,RESERVATION_RESERVATIONARN,RESERVEDHOURS,USED_HOURS,UNUSED_HOURS,WASTAGE_COST,
RI_UTILIZATION,MYCLOUD_STARTMONTH,MYCLOUD_STARTYEAR,INSTANCETYPE,NFAPPLIED,QUANTITY,RI_ECOST
)
select
RESERVATION_ID ,
PAYER_NAME,
a.BILL_PAYERACCOUNTID,
LINKEDACCOUNTID,
FAMILY_TYPE,
CUSTOMER_NAME,
REGION,
'RDS' as product_name,
OPERATINGSYSTEM,
INSTANCETYPE_FAMILY,
'' as SAVINGSPLAN_TYPE,
STARTDATE,
TOTAL_NU,
USED_NU,
total_nu - used_nu as unused_nu,
unused_nu/total_nu as wastage_percentage,
OFFERING_TYPE,
RESERVATION_RESERVATIONARN,
RESERVEDHOURS,
USEDHOURS/NFAPPLIED as actual_used_hours,
reservedHours - actual_used_hours as unused_hours,
(reservedHours - actual_used_hours)*ri_ecost as wastage_cost,
(actual_used_hours/reservedHours)*1 as ri_utilization,
MYCLOUD_STARTMONTH,
MYCLOUD_STARTYEAR,
INSTANCETYPE,
NFAPPLIED,
QUANTITY,
RI_ECOST
from rds_ri_utilization_report_hourly_temp a
left join rds_payer_max_date b on a.BILL_PAYERACCOUNTID = b.BILL_PAYERACCOUNTID 
where 
a.STARTDATE <= b.max_date and 
reservedHours <> 0;




-------------------- Elasticache Wastage ------------------------------------------

CREATE OR REPLACE TEMPORARY TABLE Elasticache_payer_max_date as
select
bill_payer_account_id as bill_payeraccountid,
DATEADD(SECOND, -1, max(line_item_usage_start_date)) AS MAX_DATE
from analytics_application_table_#startyear_#startmonth t,
      LATERAL FLATTEN(input => t.product:key_value) f
WHERE
t.line_item_line_item_type = 'DiscountedUsage' 
and f.value:key in ('product_name')
AND f.value:value = 'Amazon ElastiCache'
and bill_payer_account_id in (#payers_ids)
group by 1;


CREATE OR REPLACE TEMPORARY TABLE elasticache_ri_report_hourly_temp AS
WITH rifee_data AS (
    SELECT
        distinct
         t.reservation_reservation_a_r_n,
        SPLIT_PART(t.LINE_ITEM_USAGE_TYPE, ':', 2) AS instance_type,
        case
         when t.LINE_ITEM_USAGE_TYPE like '%.nano%' then 0.25
                            when t.LINE_ITEM_USAGE_TYPE like '%.micro%'  then 0.5
                            when t.LINE_ITEM_USAGE_TYPE like '%.small%'  then 1
                            when t.LINE_ITEM_USAGE_TYPE like '%.medium%'  then 2
                            when t.LINE_ITEM_USAGE_TYPE like '%.16xl%'  then 128
                            when t.LINE_ITEM_USAGE_TYPE like '%.18xl%' then 144
                            when t.LINE_ITEM_USAGE_TYPE like '%.24xl%'  then 192
                            when t.LINE_ITEM_USAGE_TYPE like '%.32xl%'  then 256
                            when t.LINE_ITEM_USAGE_TYPE like '%.12xl%' then 96
                            when t.LINE_ITEM_USAGE_TYPE like '%.10xl%'  then 80
                            when t.LINE_ITEM_USAGE_TYPE like '%.9xl%' then 72
                            when t.LINE_ITEM_USAGE_TYPE like '%.8xl%' then 64
                            when t.LINE_ITEM_USAGE_TYPE like '%.6xl%' then 48
                            when t.LINE_ITEM_USAGE_TYPE like '%.4xl%' then 32
                            when t.LINE_ITEM_USAGE_TYPE like '%.3xl%' then 24
                            when t.LINE_ITEM_USAGE_TYPE like '%.2xl%' then 16
                            when t.LINE_ITEM_USAGE_TYPE like '%.xl%' then 8
                            when t.LINE_ITEM_USAGE_TYPE like '%.l%'  then 4
                            else line_item_normalization_factor
                            end as nfactor_used,
        case
    		WHEN line_item_operation = 'CreateCacheCluster:0002' THEN 'Redis' 
    		WHEN line_item_operation = 'CreateCacheCluster:0001' THEN 'Memcached' 
    		WHEN line_item_operation = 'CreateCacheCluster:Valkey' THEN 'Valkey' 
        end as mycloud_operatingsystem,
        instance_type as  MYCLOUD_FAMILY_FLEXIBLE,
            MAX(CASE WHEN f.value:key = 'region' THEN f.value:value::STRING END) AS region_code,
                MAX(CASE WHEN f.value:key = 'product_name' THEN f.value:value::STRING END) AS product_productname,
        	case
	WHEN region_code like 'us-east-2%' OR LINE_ITEM_USAGE_TYPE like '%USE2-%' 
	THEN 'Ohio'
	WHEN region_code like 'us-east-1%'OR LINE_ITEM_USAGE_TYPE like '%USE1-%' 
	THEN 'N. Virginia'
	WHEN region_code like 'us-west-1%' OR LINE_ITEM_USAGE_TYPE like '%USW1-%' 
	THEN 'N. California'
	WHEN region_code like 'us-west-2%' OR LINE_ITEM_USAGE_TYPE like '%USW2-%' 
	THEN 'Oregon'
	WHEN region_code like 'ap-east-1%' OR LINE_ITEM_USAGE_TYPE like '%APE1-%' 
	THEN 'Hong Kong'
	WHEN region_code like 'ap-south-1%' OR LINE_ITEM_USAGE_TYPE like '%APS3-%' 
	THEN 'Mumbai'
	WHEN region_code like 'ap-northeast-3%' OR LINE_ITEM_USAGE_TYPE like '%APN3-%' 
	THEN 'Osaka'
	WHEN region_code like 'ap-northeast-2%' OR LINE_ITEM_USAGE_TYPE like '%APN2-%' 
	THEN 'Seoul'
	WHEN region_code like 'ap-southeast-1%' OR LINE_ITEM_USAGE_TYPE like '%APS1-%' 
	THEN 'Singapore'
	WHEN region_code like 'ap-southeast-2%' OR LINE_ITEM_USAGE_TYPE like '%APS2-%' 
	THEN 'Sydney'
	WHEN region_code like 'ap-southeast-3%' OR LINE_ITEM_USAGE_TYPE like '%APS4-%' 
	THEN 'Jakarta'
	WHEN region_code like 'ap-northeast-1%' OR LINE_ITEM_USAGE_TYPE like '%APN1-%' 
	THEN 'Tokyo'
	WHEN region_code like 'ca-central-1%' OR LINE_ITEM_USAGE_TYPE like '%CAN1-%' 
	THEN 'Central'
	WHEN region_code like 'eu-west-1%' OR LINE_ITEM_USAGE_TYPE like '%EU-%' 
	THEN 'Ireland'
	WHEN region_code like 'eu-west-1%' OR LINE_ITEM_USAGE_TYPE like '%EUW1-%' 
	THEN 'Ireland'
	WHEN region_code like 'eu-central-1%' OR LINE_ITEM_USAGE_TYPE like '%EUC1-%' 
	THEN 'Frankfurt'
	WHEN region_code like 'eu-west-2%' OR LINE_ITEM_USAGE_TYPE like '%EUW2-%' 
	THEN 'London'
	WHEN region_code like 'eu-west-3%' OR LINE_ITEM_USAGE_TYPE like '%EUW3-%' 
	THEN 'Paris'
	WHEN region_code like 'eu-north-1%' OR LINE_ITEM_USAGE_TYPE like '%EUN1-%' 
	THEN 'Stockholm'
	WHEN region_code like 'me-south-1%' OR LINE_ITEM_USAGE_TYPE like '%MES1-%' 
	THEN 'Bahrain'
	WHEN region_code like 'sa-east-1%' OR LINE_ITEM_USAGE_TYPE like '%SAE1-%' 
	THEN 'Sao Paulo'
	WHEN region_code like 'ug-west-1%' OR LINE_ITEM_USAGE_TYPE like '%UGW1-%' 
	THEN 'GovCloud (US)'
	WHEN region_code like 'ug-east-1%' OR LINE_ITEM_USAGE_TYPE like '%UGE1-%' 
	THEN 'GovCloud (US-East)'
	WHEN region_code like 'af-south-1%' OR LINE_ITEM_USAGE_TYPE like '%AFS1-%' 
	THEN 'Cape Town'
 	WHEN region_code like 'eu-south-1%' or LINE_ITEM_USAGE_TYPE like '%EUS1-%' 
    THEN 'Milan'
    WHEN region_code like 'me-central-1%' or LINE_ITEM_USAGE_TYPE like '%MEC1-%' 
    THEN 'UAE'
    WHEN region_code like 'ap-south-2%' or LINE_ITEM_USAGE_TYPE like '%APS2-%' 
    THEN 'Hyderabad'
	else ''
	end as mycloud_region,
    t.bill_payer_account_id,
    t.line_item_usage_account_id,
    FROM
        analytics_application_table_#startyear_#startmonth t,
      LATERAL FLATTEN(input => t.product:key_value) f

    WHERE
 extract(month from t.line_item_usage_start_date) = #startmonth
and extract(year from t.line_item_usage_start_date) = #startyear
	and line_item_line_item_type  in ('RIFee')
    and bill_payer_account_id in (#payers_ids)

GROUP BY 
    t.reservation_reservation_a_r_n,
    t.LINE_ITEM_USAGE_TYPE,
    nfactor_used,
    t.line_item_operation,
    t.bill_payer_account_id,
    t.line_item_usage_account_id
HAVING MAX(CASE WHEN f.value:key = 'product_name' THEN f.value:value::STRING END) = 'Amazon ElastiCache'
	
)

SELECT
    SPLIT_PART(m.RESERVATION_RESERVATION_A_R_N, ':', 7) AS reservation_id,
    line_item_usage_start_date as lineitem_usagestartdate,
    case when line_item_line_item_type = 'RIFee' then 
	DATEADD(SECOND, 1, line_item_usage_end_date) 
	else  line_item_usage_end_date end as lineitem_usageenddate,
    m.bill_payer_account_id as bill_payeraccountid,
	b.payer_name,
    r.line_item_usage_account_id as lineitem_usageaccountid,
    r.mycloud_family_flexible AS instancetype_family,
    r.INSTANCE_TYPE as instancetype,
      extract(month from line_item_usage_start_date) as mycloud_startmonth,
    extract(year from line_item_usage_start_date) as mycloud_startyear,
    r.product_productname,
    r.mycloud_operatingsystem,
    r.mycloud_region,
    pricing_offering_class AS offering_type,
    line_item_line_item_type as lineitem_lineitemtype,
    m.RESERVATION_RESERVATION_A_R_N as RESERVATION_RESERVATIONARN,
    reservation_number_of_reservations as reservation_numberofreservations,
 SUM(
    CASE 
        WHEN line_item_line_item_type = 'RIFee' THEN 
            CASE 
                WHEN line_item_normalization_factor = 0 or line_item_normalization_factor is null  THEN r.nfactor_used
                ELSE line_item_normalization_factor
            END
        ELSE 0
    END
) AS lineitem_normalizationfactor,
    SUM( case 
            when line_item_normalized_usage_amount = 0 or line_item_normalized_usage_amount  is null
            then r.nfactor_used * line_item_usage_amount
            else line_item_normalized_usage_amount
            end
        ) AS lineitem_normalizedusageamount,
    SUM(line_item_unblended_cost) AS lineitem_unblendedcost,
    SUM(reservation_effective_cost) AS reservation_EffectiveCost,
    SUM(
        CASE
            WHEN pricing_public_on_demand_cost = 0 THEN 0
            ELSE pricing_public_on_demand_cost::DECIMAL(30,8)
        END
    ) AS mycloud_ondemandprice,
    SUM(
        CASE
            WHEN line_item_line_item_type = 'RIFee' THEN (
                COALESCE(line_item_unblended_rate::REAL, 0) +
                CASE
                    WHEN DATEDIFF(hour, line_item_usage_start_date, DATEADD(second, 2, line_item_usage_end_date)) = 0 THEN 0
                    ELSE (
                        reservation_amortized_upfront_fee_for_billing_period /
                        (
                            DATEDIFF(hour, line_item_usage_start_date, DATEADD(second, 2, line_item_usage_end_date)) * reservation_number_of_reservations
                        )
                    )
                END
            )
            ELSE 0
        END
    ) AS ri_hourly_effective_cost
FROM
    analytics_application_table_#startyear_#startmonth  m

LEFT JOIN
    payer_billdesk_config.payers b ON b.payer_account_id = m.bill_payer_account_id

LEFT JOIN
    rifee_data r ON 
        r.RESERVATION_RESERVATION_A_R_N = m.RESERVATION_RESERVATION_A_R_N AND
        r.bill_payer_account_id = m.bill_payer_account_id 
WHERE
   extract(month from m.line_item_usage_start_date) = #startmonth
   and extract(year from m.line_item_usage_start_date) = #startyear	
    and m.bill_payer_account_id in (#payers_ids)
    and lineitem_lineitemtype  in ('DiscountedUsage','VDiscountedUsage','RIFee')
	group by all;




CREATE OR REPLACE TEMPORARY TABLE elasticache_ri_utilization_report_hourly_temp AS
WITH RECURSIVE hourSequence AS (
  SELECT 0 AS seq
  UNION ALL
  SELECT seq + 1
  FROM hourSequence
  WHERE seq < 24*DATEDIFF(DAY, date_from_parts(#startyear, #startmonth, 1), dateadd('day',1,last_day(date_from_parts(#startyear ,#startmonth,1)))) - 1
)
select
    RESERVATION_ID,
    bill_payeraccountid,
    payer_name,
    a.family_type ,
    a.customer_name ,
	lineitem_usageaccountid as linkedaccountid,
	DATEADD(hour, seq,date_from_parts(#startyear, #startmonth, 1)) AS startdate,
    DATEADD(second, -1, DATEADD(hour, 1, DATEADD(hour, seq, date_from_parts(#startyear, #startmonth, 1)))) AS enddate,
	reservation_reservationarn as reservation_reservationarn,
	mycloud_startmonth,
	mycloud_startyear,
	mycloud_region as region,
    offering_type,
	mycloud_operatingsystem as operatingSystem,
    INSTANCETYPE_FAMILY,
	instancetype,
    sum(lineitem_normalizationfactor) as NFapplied,
	coalesce(sum(mycloud_ondemandprice), 0) as onDemandCost,
	sum(case when lineitem_lineitemtype in('RIFee') then reservation_numberofreservations*(DATEDIFF(hour, greatest(lineitem_usagestartdate, cast(startDate as timestamp) ), dateadd(second, 2, least(lineitem_usageenddate, cast(endDate as timestamp))) )) else 0 end) as reservedHours,
    sum(case when lineitem_lineitemtype in ('RIFee') then reservation_numberofreservations*lineitem_normalizationfactor else 0 end) as total_NU,
	sum(case when lineitem_lineitemtype in ('RIFee') then reservation_numberofreservations else 0 end) as quantity,
    sum(case when lineitem_lineitemtype in ('DiscountedUsage', 'VDiscountedUsage')
		then lineitem_normalizedusageamount
	    else 0 end) as Used_NU,
	sum(case when lineitem_lineitemtype in ('DiscountedUsage', 'VDiscountedUsage')
		then mycloud_ondemandprice
	    else 0 end) as od_cost,
	sum(case when lineitem_lineitemtype in ('RIFee')
		then ri_hourly_effective_cost
		else 0 end) as ri_ecost,
	sum(case when lineitem_lineitemtype in ('DiscountedUsage', 'VDiscountedUsage')
		then mycloud_ondemandprice
	    else 0 end) - sum(case when lineitem_lineitemtype in ('RIFee')
		then ri_hourly_effective_cost
		else 0 end)*reservedHours as netSavings,
    sum(case when(lineitem_lineitemtype in('DiscountedUsage', 'VDiscountedUsage') and lineitem_usagestartdate>= cast(startDate as timestamp) and lineitem_usagestartdate<= cast(endDate as timestamp)) then (lineitem_normalizedusageamount) else 0 end) as usedHours
from
	elasticache_ri_report_hourly_temp a2
cross join hourSequence wtp

    join (select * from payer_billdesk_config.account) a on split_part(a2.reservation_reservationarn, ':', 5) =a.account_id
where
		mycloud_startmonth= #startmonth
	AND mycloud_startyear= #startyear
  and	((( cast(startDate as timestamp) between lineitem_usagestartdate and lineitem_usageenddate
	or cast(endDate as timestamp) between lineitem_usagestartdate and lineitem_usageenddate
 or
				            lineitem_usagestartdate
					between
					cast(startDate as timestamp)
        					and
        					cast(endDate as timestamp))
	and datediff(hour,cast(startdate as timestamp),lineitem_usageenddate ) != 0
	and lineitem_lineitemtype in ('RIFee'))
	or (lineitem_lineitemtype in ('DiscountedUsage', 'VDiscountedUsage')
	and lineitem_usagestartdate>= cast(startDate as timestamp)
	and lineitem_usagestartdate<= cast(endDate as timestamp)))
	and product_productname = 'Amazon ElastiCache'
	and reservation_reservationarn != ''
	and lineitem_lineitemtype in ('RIFee', 'DiscountedUsage', 'VDiscountedUsage')
	group by all;




DELETE
FROM ck_analytics_application_ri_wastage_hourly
WHERE
extract(month from lineitem_usagestartdate) = #startmonth
and extract(year from lineitem_usagestartdate) = #startyear
and bill_payeraccountid in (#payers_ids)
and product_name = 'ElastiCache';


insert into ck_analytics_application_ri_wastage_hourly (
RESERVATION_ARN,PAYER_NAME,BILL_PAYERACCOUNTID,USAGEACCOUNTID,ACCOUNT_TYPE,CUSTOMER_NAME,REGION,PRODUCT_NAME,
OPERATINGSYSTEM_ENGINE,INSTANCETYPE_FAMILY,SAVINGSPLAN_TYPE,LINEITEM_USAGESTARTDATE,TOTAL_NU,USED_NU,UNUSED_NU,
WASTAGE_PERCENTAGE,OFFERING_TYPE,RESERVATION_RESERVATIONARN,RESERVEDHOURS,USED_HOURS,UNUSED_HOURS,WASTAGE_COST,
RI_UTILIZATION,MYCLOUD_STARTMONTH,MYCLOUD_STARTYEAR,INSTANCETYPE,NFAPPLIED,QUANTITY,RI_ECOST
)
select
RESERVATION_ID ,
PAYER_NAME,
a.BILL_PAYERACCOUNTID,
LINKEDACCOUNTID,
FAMILY_TYPE,
CUSTOMER_NAME,
REGION,
'ElastiCache' as product_name,
OPERATINGSYSTEM,
INSTANCETYPE_FAMILY,
'' as SAVINGSPLAN_TYPE,
STARTDATE,
TOTAL_NU,
USED_NU,
total_nu - used_nu as unused_nu,
unused_nu/total_nu as wastage_percentage,
OFFERING_TYPE,
RESERVATION_RESERVATIONARN,
RESERVEDHOURS,
USEDHOURS/NFAPPLIED as actual_used_hours,
reservedHours - actual_used_hours as unused_hours,
(reservedHours - actual_used_hours)*ri_ecost as wastage_cost,
(actual_used_hours/reservedHours)*1 as ri_utilization,
MYCLOUD_STARTMONTH,
MYCLOUD_STARTYEAR,
INSTANCETYPE,
NFAPPLIED,
QUANTITY,
RI_ECOST
from elasticache_ri_utilization_report_hourly_temp a
left join Elasticache_payer_max_date b on a.BILL_PAYERACCOUNTID = b.BILL_PAYERACCOUNTID 
where 
a.STARTDATE <= b.max_date and 
reservedHours <> 0;

---------------------- ElasticSearch  ------------------------------ 

CREATE OR REPLACE TEMPORARY TABLE ElasticSearch_payer_max_date as
select
bill_payer_account_id as bill_payeraccountid,
DATEADD(SECOND, -1, max(line_item_usage_start_date)) AS MAX_DATE
from analytics_application_table_#startyear_#startmonth t,
      LATERAL FLATTEN(input => t.product:key_value) f
WHERE
t.line_item_line_item_type = 'DiscountedUsage' 
and f.value:key in ('product_name')
AND f.value:value in ('Amazon Elasticsearch Service','Amazon OpenSearch Service')
and bill_payer_account_id in (#payers_ids)
group by 1;


CREATE OR REPLACE TEMPORARY TABLE es_ri_report_hourly_temp AS
WITH rifee_data AS (
    SELECT
        distinct
         t.reservation_reservation_a_r_n,
        SPLIT_PART(t.LINE_ITEM_USAGE_TYPE, ':', 2) AS instance_type,
        case
         when t.LINE_ITEM_USAGE_TYPE like '%.nano%' then 0.25
                            when t.LINE_ITEM_USAGE_TYPE like '%.micro%'  then 0.5
                            when t.LINE_ITEM_USAGE_TYPE like '%.small%'  then 1
                            when t.LINE_ITEM_USAGE_TYPE like '%.medium%'  then 2
                            when t.LINE_ITEM_USAGE_TYPE like '%.16xl%'  then 128
                            when t.LINE_ITEM_USAGE_TYPE like '%.18xl%' then 144
                            when t.LINE_ITEM_USAGE_TYPE like '%.24xl%'  then 192
                            when t.LINE_ITEM_USAGE_TYPE like '%.32xl%'  then 256
                            when t.LINE_ITEM_USAGE_TYPE like '%.12xl%' then 96
                            when t.LINE_ITEM_USAGE_TYPE like '%.10xl%'  then 80
                            when t.LINE_ITEM_USAGE_TYPE like '%.9xl%' then 72
                            when t.LINE_ITEM_USAGE_TYPE like '%.8xl%' then 64
                            when t.LINE_ITEM_USAGE_TYPE like '%.6xl%' then 48
                            when t.LINE_ITEM_USAGE_TYPE like '%.4xl%' then 32
                            when t.LINE_ITEM_USAGE_TYPE like '%.3xl%' then 24
                            when t.LINE_ITEM_USAGE_TYPE like '%.2xl%' then 16
                            when t.LINE_ITEM_USAGE_TYPE like '%.xl%' then 8
                            when t.LINE_ITEM_USAGE_TYPE like '%.l%'  then 4
                            else line_item_normalization_factor
                            end as nfactor_used,
       '' mycloud_operatingsystem,
        instance_type as  MYCLOUD_FAMILY_FLEXIBLE,
            MAX(CASE WHEN f.value:key = 'region' THEN f.value:value::STRING END) AS region_code,
                MAX(CASE WHEN f.value:key = 'product_name' THEN f.value:value::STRING END) AS product_productname,
        	case
	WHEN region_code like 'us-east-2%' OR LINE_ITEM_USAGE_TYPE like '%USE2-%' 
	THEN 'Ohio'
	WHEN region_code like 'us-east-1%'OR LINE_ITEM_USAGE_TYPE like '%USE1-%' 
	THEN 'N. Virginia'
	WHEN region_code like 'us-west-1%' OR LINE_ITEM_USAGE_TYPE like '%USW1-%' 
	THEN 'N. California'
	WHEN region_code like 'us-west-2%' OR LINE_ITEM_USAGE_TYPE like '%USW2-%' 
	THEN 'Oregon'
	WHEN region_code like 'ap-east-1%' OR LINE_ITEM_USAGE_TYPE like '%APE1-%' 
	THEN 'Hong Kong'
	WHEN region_code like 'ap-south-1%' OR LINE_ITEM_USAGE_TYPE like '%APS3-%' 
	THEN 'Mumbai'
	WHEN region_code like 'ap-northeast-3%' OR LINE_ITEM_USAGE_TYPE like '%APN3-%' 
	THEN 'Osaka'
	WHEN region_code like 'ap-northeast-2%' OR LINE_ITEM_USAGE_TYPE like '%APN2-%' 
	THEN 'Seoul'
	WHEN region_code like 'ap-southeast-1%' OR LINE_ITEM_USAGE_TYPE like '%APS1-%' 
	THEN 'Singapore'
	WHEN region_code like 'ap-southeast-2%' OR LINE_ITEM_USAGE_TYPE like '%APS2-%' 
	THEN 'Sydney'
	WHEN region_code like 'ap-southeast-3%' OR LINE_ITEM_USAGE_TYPE like '%APS4-%' 
	THEN 'Jakarta'
	WHEN region_code like 'ap-northeast-1%' OR LINE_ITEM_USAGE_TYPE like '%APN1-%' 
	THEN 'Tokyo'
	WHEN region_code like 'ca-central-1%' OR LINE_ITEM_USAGE_TYPE like '%CAN1-%' 
	THEN 'Central'
	WHEN region_code like 'eu-west-1%' OR LINE_ITEM_USAGE_TYPE like '%EU-%' 
	THEN 'Ireland'
	WHEN region_code like 'eu-west-1%' OR LINE_ITEM_USAGE_TYPE like '%EUW1-%' 
	THEN 'Ireland'
	WHEN region_code like 'eu-central-1%' OR LINE_ITEM_USAGE_TYPE like '%EUC1-%' 
	THEN 'Frankfurt'
	WHEN region_code like 'eu-west-2%' OR LINE_ITEM_USAGE_TYPE like '%EUW2-%' 
	THEN 'London'
	WHEN region_code like 'eu-west-3%' OR LINE_ITEM_USAGE_TYPE like '%EUW3-%' 
	THEN 'Paris'
	WHEN region_code like 'eu-north-1%' OR LINE_ITEM_USAGE_TYPE like '%EUN1-%' 
	THEN 'Stockholm'
	WHEN region_code like 'me-south-1%' OR LINE_ITEM_USAGE_TYPE like '%MES1-%' 
	THEN 'Bahrain'
	WHEN region_code like 'sa-east-1%' OR LINE_ITEM_USAGE_TYPE like '%SAE1-%' 
	THEN 'Sao Paulo'
	WHEN region_code like 'ug-west-1%' OR LINE_ITEM_USAGE_TYPE like '%UGW1-%' 
	THEN 'GovCloud (US)'
	WHEN region_code like 'ug-east-1%' OR LINE_ITEM_USAGE_TYPE like '%UGE1-%' 
	THEN 'GovCloud (US-East)'
	WHEN region_code like 'af-south-1%' OR LINE_ITEM_USAGE_TYPE like '%AFS1-%' 
	THEN 'Cape Town'
 	WHEN region_code like 'eu-south-1%' or LINE_ITEM_USAGE_TYPE like '%EUS1-%' 
    THEN 'Milan'
    WHEN region_code like 'me-central-1%' or LINE_ITEM_USAGE_TYPE like '%MEC1-%' 
    THEN 'UAE'
    WHEN region_code like 'ap-south-2%' or LINE_ITEM_USAGE_TYPE like '%APS2-%' 
    THEN 'Hyderabad'
	else ''
	end as mycloud_region,
    t.bill_payer_account_id,
    t.line_item_usage_account_id,
    FROM
        analytics_application_table_#startyear_#startmonth t,
      LATERAL FLATTEN(input => t.product:key_value) f

    WHERE
 extract(month from t.line_item_usage_start_date) = #startmonth
and extract(year from t.line_item_usage_start_date) = #startyear
	and line_item_line_item_type  in ('RIFee')
    and bill_payer_account_id in (#payers_ids)

GROUP BY 
    t.reservation_reservation_a_r_n,
    t.LINE_ITEM_USAGE_TYPE,
    nfactor_used,
    t.line_item_operation,
    t.bill_payer_account_id,
    t.line_item_usage_account_id
HAVING MAX(CASE WHEN f.value:key = 'product_name' THEN f.value:value::STRING END) in ('Amazon Elasticsearch Service','Amazon OpenSearch Service')
	
)

SELECT
    SPLIT_PART(m.RESERVATION_RESERVATION_A_R_N, '/', 2) AS reservation_id,
    line_item_usage_start_date as lineitem_usagestartdate,
    case when line_item_line_item_type = 'RIFee' then 
	DATEADD(SECOND, 1, line_item_usage_end_date) 
	else  line_item_usage_end_date end as lineitem_usageenddate,
    m.bill_payer_account_id as bill_payeraccountid,
	b.payer_name,
    r.line_item_usage_account_id as lineitem_usageaccountid,
    r.mycloud_family_flexible AS instancetype_family,
    r.INSTANCE_TYPE as instancetype,
      extract(month from line_item_usage_start_date) as mycloud_startmonth,
    extract(year from line_item_usage_start_date) as mycloud_startyear,
    r.product_productname,
    r.mycloud_operatingsystem,
    r.mycloud_region,
    pricing_offering_class AS offering_type,
    line_item_line_item_type as lineitem_lineitemtype,
    m.RESERVATION_RESERVATION_A_R_N as RESERVATION_RESERVATIONARN,
    reservation_number_of_reservations as reservation_numberofreservations,
 SUM(
    CASE 
        WHEN line_item_line_item_type = 'RIFee' THEN 
            CASE 
                WHEN line_item_normalization_factor = 0 or line_item_normalization_factor is null  THEN r.nfactor_used
                ELSE line_item_normalization_factor
            END
        ELSE 0
    END
) AS lineitem_normalizationfactor,
    SUM( case 
            when line_item_normalized_usage_amount = 0 or line_item_normalized_usage_amount  is null
            then r.nfactor_used * line_item_usage_amount
            else line_item_normalized_usage_amount
            end
        ) AS lineitem_normalizedusageamount,
    SUM(line_item_unblended_cost) AS lineitem_unblendedcost,
    sum(line_item_usage_amount) as lineitem_usageamount,

    SUM(reservation_effective_cost) AS reservation_EffectiveCost,
    SUM(
        CASE
            WHEN pricing_public_on_demand_cost = 0 THEN 0
            ELSE pricing_public_on_demand_cost::DECIMAL(30,8)
        END
    ) AS mycloud_ondemandprice,
    SUM(
        CASE
            WHEN line_item_line_item_type = 'RIFee' THEN (
                COALESCE(line_item_unblended_rate::REAL, 0) +
                CASE
                    WHEN DATEDIFF(hour, line_item_usage_start_date, DATEADD(second, 2, line_item_usage_end_date)) = 0 THEN 0
                    ELSE (
                        reservation_amortized_upfront_fee_for_billing_period /
                        (
                            DATEDIFF(hour, line_item_usage_start_date, DATEADD(second, 2, line_item_usage_end_date)) * reservation_number_of_reservations
                        )
                    )
                END
            )
            ELSE 0
        END
    ) AS ri_hourly_effective_cost
FROM
    analytics_application_table_#startyear_#startmonth  m

LEFT JOIN
    payer_billdesk_config.payers b ON b.payer_account_id = m.bill_payer_account_id

LEFT JOIN
    rifee_data r ON 
        r.RESERVATION_RESERVATION_A_R_N = m.RESERVATION_RESERVATION_A_R_N AND
        r.bill_payer_account_id = m.bill_payer_account_id 
WHERE
   extract(month from m.line_item_usage_start_date) = #startmonth
   and extract(year from m.line_item_usage_start_date) = #startyear	
    and m.bill_payer_account_id in (#payers_ids)
	and lineitem_lineitemtype  in ('DiscountedUsage','VDiscountedUsage','RIFee')
	group by all;



CREATE OR REPLACE TEMPORARY TABLE es_ri_utilization_report_hourly_temp AS 
 WITH RECURSIVE hourSequence AS (
  SELECT 0 AS seq
  UNION ALL
  SELECT seq + 1
  FROM hourSequence
WHERE seq < 24*DATEDIFF(DAY, date_from_parts(#startyear	, #startmonth, 1),dateadd('day',1,last_day(date_from_parts(#startyear , #startmonth,1)))) - 1)
select
    RESERVATION_ID,
    bill_payeraccountid,
    
    b.payer_name,
    a.family_type ,
    a.customer_name ,
    
	lineitem_usageaccountid as linkedaccountid,
    DATEADD(hour, seq,date_from_parts(#startyear, #startmonth, 1)) AS startdate,
    DATEADD(second, -1, DATEADD(hour, 1, DATEADD(hour, seq, date_from_parts(#startyear, #startmonth, 1)))) AS enddate,
    reservation_reservationarn as reservation_reservationarn,
	mycloud_startmonth,
	mycloud_startyear,
	mycloud_region as region,
    offering_type,
	mycloud_operatingsystem as operatingSystem,
	instancetype as instanceType,
    sum(lineitem_normalizationfactor) as NFapplied,
	coalesce(sum(mycloud_ondemandprice), 0) as onDemandCost,
    sum(case when lineitem_lineitemtype in('RIFee') then reservation_numberofreservations*(DATEDIFF(hour, greatest(lineitem_usagestartdate, cast(startDate as timestamp) ), dateadd(second, 2, least(lineitem_usageenddate, cast(endDate as timestamp))) )) else 0 end) as reservedHours,
       sum(case when lineitem_lineitemtype in ('RIFee') then reservation_numberofreservations else 0 end) as total_NU,
	sum(case when lineitem_lineitemtype in ('RIFee') then reservation_numberofreservations else 0 end) as quantity,
     sum(case when lineitem_lineitemtype in ('DiscountedUsage', 'VDiscountedUsage')
		then lineitem_usageamount
	    else 0 end) as Used_NU,
sum(case when lineitem_lineitemtype in ('DiscountedUsage', 'VDiscountedUsage')
		then mycloud_ondemandprice
	    else 0 end) as od_cost,
	sum(case when lineitem_lineitemtype in ('RIFee')
		then ri_hourly_effective_cost
		else 0 end) as ri_ecost,
	sum(case when lineitem_lineitemtype in ('DiscountedUsage', 'VDiscountedUsage')
		then mycloud_ondemandprice
	    else 0 end) - sum(case when lineitem_lineitemtype in ('RIFee')
		then ri_hourly_effective_cost
		else 0 end)*reservedHours as netSavings,
	sum(case when(lineitem_lineitemtype in('DiscountedUsage', 'VDiscountedUsage') and lineitem_usagestartdate>= cast(startDate as timestamp) and lineitem_usagestartdate<= cast(endDate as timestamp) ) then lineitem_usageamount else 0 end) as usedHours
from
	es_ri_report_hourly_temp a2
    LEFT JOIN
    payer_billdesk_config.payers b ON b.payer_account_id = a2.bill_payeraccountid
	cross join hourSequence wtp
	

    join (select * from payer_billdesk_config.account) a on split_part(a2.reservation_reservationarn, ':', 5) =a.account_id

where 
		mycloud_startmonth= #startmonth
	AND mycloud_startyear= #startyear
	and ((( cast(startDate as timestamp) between lineitem_usagestartdate and lineitem_usageenddate
	or cast(endDate as timestamp) between lineitem_usagestartdate and lineitem_usageenddate
 or 
				            lineitem_usagestartdate 
					between 
					cast(startDate as timestamp)
        					and 
        					cast(endDate as timestamp))
	and datediff(hour,cast(startdate as timestamp),lineitem_usageenddate ) != 0
	and lineitem_lineitemtype in ('RIFee'))
	or (lineitem_lineitemtype in ('DiscountedUsage', 'VDiscountedUsage')
	and lineitem_usagestartdate>= cast(startDate as timestamp)
	and lineitem_usagestartdate<= cast(endDate as timestamp) ))
    and reservation_reservationarn != ''
    and product_productname in ('Amazon Elasticsearch Service', 'Amazon OpenSearch Service')
	and lineitem_lineitemtype in ('RIFee', 'DiscountedUsage', 'VDiscountedUsage')
group by all;



DELETE
FROM ck_analytics_application_ri_wastage_hourly
WHERE
extract(month from lineitem_usagestartdate) = #startmonth
and extract(year from lineitem_usagestartdate) = #startyear
and bill_payeraccountid in  (#payers_ids)
and product_name = 'OpenSearch';

    
insert into ck_analytics_application_ri_wastage_hourly (
RESERVATION_ARN,PAYER_NAME,BILL_PAYERACCOUNTID,USAGEACCOUNTID,ACCOUNT_TYPE,CUSTOMER_NAME,REGION,PRODUCT_NAME,
OPERATINGSYSTEM_ENGINE,INSTANCETYPE_FAMILY,SAVINGSPLAN_TYPE,LINEITEM_USAGESTARTDATE,TOTAL_NU,USED_NU,UNUSED_NU,
WASTAGE_PERCENTAGE,OFFERING_TYPE,RESERVATION_RESERVATIONARN,RESERVEDHOURS,USED_HOURS,UNUSED_HOURS,WASTAGE_COST,
RI_UTILIZATION,MYCLOUD_STARTMONTH,MYCLOUD_STARTYEAR,INSTANCETYPE,NFAPPLIED,QUANTITY,RI_ECOST
)
select
RESERVATION_ID ,
PAYER_NAME,
a.BILL_PAYERACCOUNTID,
LINKEDACCOUNTID,
FAMILY_TYPE,
CUSTOMER_NAME,
REGION,
'OpenSearch' as product_name,
OPERATINGSYSTEM,
instanceType,
'' as SAVINGSPLAN_TYPE,
STARTDATE,
TOTAL_NU,
USED_NU,
total_nu - used_nu as unused_nu,
unused_nu/total_nu as wastage_percentage,
OFFERING_TYPE,
RESERVATION_RESERVATIONARN,
RESERVEDHOURS,
USEDHOURS as actual_used_hours,
reservedHours - actual_used_hours as unused_hours,
(reservedHours - actual_used_hours)*ri_ecost as wastage_cost,
(actual_used_hours/reservedHours)*1 as ri_utilization,
MYCLOUD_STARTMONTH,
MYCLOUD_STARTYEAR,
instanceType,
NFAPPLIED,
QUANTITY,
RI_ECOST
from es_ri_utilization_report_hourly_temp a
left join ElasticSearch_payer_max_date b on a.BILL_PAYERACCOUNTID = b.BILL_PAYERACCOUNTID 
where 
a.STARTDATE <= b.max_date and 
reservedHours <> 0;

---------------------- Redshift Wasatge -------------------------------------------


CREATE OR REPLACE TEMPORARY TABLE Redshift_payer_max_date as
select
bill_payer_account_id as bill_payeraccountid,
DATEADD(SECOND, -1, max(line_item_usage_start_date)) AS MAX_DATE
from analytics_application_table_#startyear_#startmonth t,
      LATERAL FLATTEN(input => t.product:key_value) f
WHERE
t.line_item_line_item_type = 'DiscountedUsage' 
and f.value:key in ('product_name')
AND f.value:value in ('Amazon Redshift')
and bill_payer_account_id in (#payers_ids)
group by 1;


CREATE OR REPLACE TEMPORARY TABLE redshift_ri_util_report_temp AS
WITH rifee_data AS (
    SELECT
        distinct
         t.reservation_reservation_a_r_n,
        SPLIT_PART(t.LINE_ITEM_USAGE_TYPE, ':', 2) AS instance_type,
        case
         when t.LINE_ITEM_USAGE_TYPE like '%.nano%' then 0.25
                            when t.LINE_ITEM_USAGE_TYPE like '%.micro%'  then 0.5
                            when t.LINE_ITEM_USAGE_TYPE like '%.small%'  then 1
                            when t.LINE_ITEM_USAGE_TYPE like '%.medium%'  then 2
                            when t.LINE_ITEM_USAGE_TYPE like '%.16xl%'  then 128
                            when t.LINE_ITEM_USAGE_TYPE like '%.18xl%' then 144
                            when t.LINE_ITEM_USAGE_TYPE like '%.24xl%'  then 192
                            when t.LINE_ITEM_USAGE_TYPE like '%.32xl%'  then 256
                            when t.LINE_ITEM_USAGE_TYPE like '%.12xl%' then 96
                            when t.LINE_ITEM_USAGE_TYPE like '%.10xl%'  then 80
                            when t.LINE_ITEM_USAGE_TYPE like '%.9xl%' then 72
                            when t.LINE_ITEM_USAGE_TYPE like '%.8xl%' then 64
                            when t.LINE_ITEM_USAGE_TYPE like '%.6xl%' then 48
                            when t.LINE_ITEM_USAGE_TYPE like '%.4xl%' then 32
                            when t.LINE_ITEM_USAGE_TYPE like '%.3xl%' then 24
                            when t.LINE_ITEM_USAGE_TYPE like '%.2xl%' then 16
                            when t.LINE_ITEM_USAGE_TYPE like '%.xl%' then 8
                            when t.LINE_ITEM_USAGE_TYPE like '%.l%'  then 4
                            else line_item_normalization_factor
                            end as nfactor_used,
       '' mycloud_operatingsystem,
        instance_type as  MYCLOUD_FAMILY_FLEXIBLE,
            MAX(CASE WHEN f.value:key = 'region' THEN f.value:value::STRING END) AS region_code,
                MAX(CASE WHEN f.value:key = 'product_name' THEN f.value:value::STRING END) AS product_productname,
        	case
	WHEN region_code like 'us-east-2%' OR LINE_ITEM_USAGE_TYPE like '%USE2-%' 
	THEN 'Ohio'
	WHEN region_code like 'us-east-1%'OR LINE_ITEM_USAGE_TYPE like '%USE1-%' 
	THEN 'N. Virginia'
	WHEN region_code like 'us-west-1%' OR LINE_ITEM_USAGE_TYPE like '%USW1-%' 
	THEN 'N. California'
	WHEN region_code like 'us-west-2%' OR LINE_ITEM_USAGE_TYPE like '%USW2-%' 
	THEN 'Oregon'
	WHEN region_code like 'ap-east-1%' OR LINE_ITEM_USAGE_TYPE like '%APE1-%' 
	THEN 'Hong Kong'
	WHEN region_code like 'ap-south-1%' OR LINE_ITEM_USAGE_TYPE like '%APS3-%' 
	THEN 'Mumbai'
	WHEN region_code like 'ap-northeast-3%' OR LINE_ITEM_USAGE_TYPE like '%APN3-%' 
	THEN 'Osaka'
	WHEN region_code like 'ap-northeast-2%' OR LINE_ITEM_USAGE_TYPE like '%APN2-%' 
	THEN 'Seoul'
	WHEN region_code like 'ap-southeast-1%' OR LINE_ITEM_USAGE_TYPE like '%APS1-%' 
	THEN 'Singapore'
	WHEN region_code like 'ap-southeast-2%' OR LINE_ITEM_USAGE_TYPE like '%APS2-%' 
	THEN 'Sydney'
	WHEN region_code like 'ap-southeast-3%' OR LINE_ITEM_USAGE_TYPE like '%APS4-%' 
	THEN 'Jakarta'
	WHEN region_code like 'ap-northeast-1%' OR LINE_ITEM_USAGE_TYPE like '%APN1-%' 
	THEN 'Tokyo'
	WHEN region_code like 'ca-central-1%' OR LINE_ITEM_USAGE_TYPE like '%CAN1-%' 
	THEN 'Central'
	WHEN region_code like 'eu-west-1%' OR LINE_ITEM_USAGE_TYPE like '%EU-%' 
	THEN 'Ireland'
	WHEN region_code like 'eu-west-1%' OR LINE_ITEM_USAGE_TYPE like '%EUW1-%' 
	THEN 'Ireland'
	WHEN region_code like 'eu-central-1%' OR LINE_ITEM_USAGE_TYPE like '%EUC1-%' 
	THEN 'Frankfurt'
	WHEN region_code like 'eu-west-2%' OR LINE_ITEM_USAGE_TYPE like '%EUW2-%' 
	THEN 'London'
	WHEN region_code like 'eu-west-3%' OR LINE_ITEM_USAGE_TYPE like '%EUW3-%' 
	THEN 'Paris'
	WHEN region_code like 'eu-north-1%' OR LINE_ITEM_USAGE_TYPE like '%EUN1-%' 
	THEN 'Stockholm'
	WHEN region_code like 'me-south-1%' OR LINE_ITEM_USAGE_TYPE like '%MES1-%' 
	THEN 'Bahrain'
	WHEN region_code like 'sa-east-1%' OR LINE_ITEM_USAGE_TYPE like '%SAE1-%' 
	THEN 'Sao Paulo'
	WHEN region_code like 'ug-west-1%' OR LINE_ITEM_USAGE_TYPE like '%UGW1-%' 
	THEN 'GovCloud (US)'
	WHEN region_code like 'ug-east-1%' OR LINE_ITEM_USAGE_TYPE like '%UGE1-%' 
	THEN 'GovCloud (US-East)'
	WHEN region_code like 'af-south-1%' OR LINE_ITEM_USAGE_TYPE like '%AFS1-%' 
	THEN 'Cape Town'
 	WHEN region_code like 'eu-south-1%' or LINE_ITEM_USAGE_TYPE like '%EUS1-%' 
    THEN 'Milan'
    WHEN region_code like 'me-central-1%' or LINE_ITEM_USAGE_TYPE like '%MEC1-%' 
    THEN 'UAE'
    WHEN region_code like 'ap-south-2%' or LINE_ITEM_USAGE_TYPE like '%APS2-%' 
    THEN 'Hyderabad'
	else ''
	end as mycloud_region,
    t.bill_payer_account_id,
    t.line_item_usage_account_id,
    FROM
        analytics_application_table_#startyear_#startmonth t,
      LATERAL FLATTEN(input => t.product:key_value) f

    WHERE
 extract(month from t.line_item_usage_start_date) = #startmonth
and extract(year from t.line_item_usage_start_date) = #startyear
	and line_item_line_item_type  in ('RIFee')
    and bill_payer_account_id in (#payers_ids)

GROUP BY 
    t.reservation_reservation_a_r_n,
    t.LINE_ITEM_USAGE_TYPE,
    nfactor_used,
    t.line_item_operation,
    t.bill_payer_account_id,
    t.line_item_usage_account_id
HAVING MAX(CASE WHEN f.value:key = 'product_name' THEN f.value:value::STRING END) = 'Amazon Redshift'
	
)

SELECT
    SPLIT_PART(m.RESERVATION_RESERVATION_A_R_N, '/', 2) AS reservation_id,
    line_item_usage_start_date as lineitem_usagestartdate,
    case when line_item_line_item_type = 'RIFee' then 
	DATEADD(SECOND, 1, line_item_usage_end_date) 
	else  line_item_usage_end_date end as lineitem_usageenddate,
    m.bill_payer_account_id as bill_payeraccountid,
	b.payer_name,
    r.line_item_usage_account_id as lineitem_usageaccountid,
    r.mycloud_family_flexible AS instancetype_family,
    r.INSTANCE_TYPE as instancetype,
      extract(month from line_item_usage_start_date) as mycloud_startmonth,
    extract(year from line_item_usage_start_date) as mycloud_startyear,
    r.product_productname,
    r.mycloud_operatingsystem,
    r.mycloud_region,
    pricing_offering_class AS offering_type,
    line_item_line_item_type as lineitem_lineitemtype,
    m.RESERVATION_RESERVATION_A_R_N as RESERVATION_RESERVATIONARN,
    reservation_number_of_reservations as reservation_numberofreservations,
 SUM(
    CASE 
        WHEN line_item_line_item_type = 'RIFee' THEN 
            CASE 
                WHEN line_item_normalization_factor = 0 or line_item_normalization_factor is null  THEN r.nfactor_used
                ELSE line_item_normalization_factor
            END
        ELSE 0
    END
) AS lineitem_normalizationfactor,
    SUM( case 
            when line_item_normalized_usage_amount = 0 or line_item_normalized_usage_amount  is null
            then r.nfactor_used * line_item_usage_amount
            else line_item_normalized_usage_amount
            end
        ) AS lineitem_normalizedusageamount,
    SUM(line_item_unblended_cost) AS lineitem_unblendedcost,
    sum(line_item_usage_amount) as lineitem_usageamount,

    SUM(reservation_effective_cost) AS reservation_EffectiveCost,
    SUM(
        CASE
            WHEN pricing_public_on_demand_cost = 0 THEN 0
            ELSE pricing_public_on_demand_cost::DECIMAL(30,8)
        END
    ) AS mycloud_ondemandprice,
    SUM(
        CASE
            WHEN line_item_line_item_type = 'RIFee' THEN (
                COALESCE(line_item_unblended_rate::REAL, 0) +
                CASE
                    WHEN DATEDIFF(hour, line_item_usage_start_date, DATEADD(second, 2, line_item_usage_end_date)) = 0 THEN 0
                    ELSE (
                        reservation_amortized_upfront_fee_for_billing_period /
                        (
                            DATEDIFF(hour, line_item_usage_start_date, DATEADD(second, 2, line_item_usage_end_date)) * reservation_number_of_reservations
                        )
                    )
                END
            )
            ELSE 0
        END
    ) AS ri_hourly_effective_cost
FROM
    analytics_application_table_#startyear_#startmonth  m

LEFT JOIN
    payer_billdesk_config.payers b ON b.payer_account_id = m.bill_payer_account_id

LEFT JOIN
    rifee_data r ON 
        r.RESERVATION_RESERVATION_A_R_N = m.RESERVATION_RESERVATION_A_R_N AND
        r.bill_payer_account_id = m.bill_payer_account_id 
WHERE
   extract(month from m.line_item_usage_start_date) = #startmonth
   and extract(year from m.line_item_usage_start_date) = #startyear 	
    and m.bill_payer_account_id in (#payers_ids)
	and lineitem_lineitemtype  in ('DiscountedUsage','VDiscountedUsage','RIFee')
	group by all;



CREATE OR REPLACE TEMPORARY TABLE redshift_ri_utilization_report_hourly_temp AS 
 WITH RECURSIVE hourSequence AS (
  SELECT 0 AS seq
  UNION ALL
  SELECT seq + 1
  FROM hourSequence
WHERE seq < 24*DATEDIFF(DAY, date_from_parts(#startyear, #startmonth, 1),dateadd('day',1,last_day(date_from_parts(#startyear ,#startmonth,1)))) - 1)
select
    RESERVATION_ID,
    bill_payeraccountid,
    
    b.payer_name,
    a.family_type ,
    a.customer_name ,
    
	lineitem_usageaccountid as linkedaccountid,
    DATEADD(hour, seq,date_from_parts(#startyear, #startmonth, 1)) AS startdate,
    DATEADD(second, -1, DATEADD(hour, 1, DATEADD(hour, seq, date_from_parts(#startyear, #startmonth, 1)))) AS enddate,
    reservation_reservationarn as reservation_reservationarn,
	mycloud_startmonth,
	mycloud_startyear,
	mycloud_region as region,
    offering_type,
	mycloud_operatingsystem as operatingSystem,
	INSTANCETYPE_FAMILY ,
    sum(lineitem_normalizationfactor) as NFapplied,
	coalesce(sum(mycloud_ondemandprice), 0) as onDemandCost,
    sum(case when lineitem_lineitemtype in('RIFee') then reservation_numberofreservations*(DATEDIFF(hour, greatest(lineitem_usagestartdate, cast(startDate as timestamp) ), dateadd(second, 2, least(lineitem_usageenddate, cast(endDate as timestamp))) )) else 0 end) as reservedHours,
       sum(case when lineitem_lineitemtype in ('RIFee') then reservation_numberofreservations else 0 end) as total_NU,
	sum(case when lineitem_lineitemtype in ('RIFee') then reservation_numberofreservations else 0 end) as quantity,
     sum(case when lineitem_lineitemtype in ('DiscountedUsage', 'VDiscountedUsage')
		then lineitem_usageamount
	    else 0 end) as Used_NU,
sum(case when lineitem_lineitemtype in ('DiscountedUsage', 'VDiscountedUsage')
		then mycloud_ondemandprice
	    else 0 end) as od_cost,
	sum(case when lineitem_lineitemtype in ('RIFee')
		then ri_hourly_effective_cost
		else 0 end) as ri_ecost,
	sum(case when lineitem_lineitemtype in ('DiscountedUsage', 'VDiscountedUsage')
		then mycloud_ondemandprice
	    else 0 end) - sum(case when lineitem_lineitemtype in ('RIFee')
		then ri_hourly_effective_cost
		else 0 end)*reservedHours as netSavings,
	sum(case when(lineitem_lineitemtype in('DiscountedUsage', 'VDiscountedUsage') and lineitem_usagestartdate>= cast(startDate as timestamp) and lineitem_usagestartdate<= cast(endDate as timestamp) ) then lineitem_usageamount else 0 end) as usedHours
from
	redshift_ri_util_report_temp a2
     LEFT JOIN
    payer_billdesk_config.payers b ON b.payer_account_id = a2.bill_payeraccountid
	cross join hourSequence wtp

    join (select * from payer_billdesk_config.account) a on split_part(a2.reservation_reservationarn, ':', 5) =a.account_id

where 
		mycloud_startmonth= #startmonth
	AND mycloud_startyear= #startyear
	and ((( cast(startDate as timestamp) between lineitem_usagestartdate and lineitem_usageenddate
	or cast(endDate as timestamp) between lineitem_usagestartdate and lineitem_usageenddate
 or 
				            lineitem_usagestartdate 
					between 
					cast(startDate as timestamp)
        					and 
        					cast(endDate as timestamp))
	and datediff(hour,cast(startdate as timestamp),lineitem_usageenddate ) != 0
	and lineitem_lineitemtype in ('RIFee'))
	or (lineitem_lineitemtype in ('DiscountedUsage', 'VDiscountedUsage')
	and lineitem_usagestartdate>= cast(startDate as timestamp)
	and lineitem_usagestartdate<= cast(endDate as timestamp) ))
    and reservation_reservationarn != ''
	and product_productname = 'Amazon Redshift'
	and lineitem_lineitemtype in ('RIFee', 'DiscountedUsage', 'VDiscountedUsage')
group by all;



DELETE
FROM ck_analytics_application_ri_wastage_hourly
WHERE
extract(month from lineitem_usagestartdate) = #startmonth
and extract(year from lineitem_usagestartdate) = #startyear
and bill_payeraccountid in (#payers_ids)
and product_name = 'Redshift';

    
insert into ck_analytics_application_ri_wastage_hourly (
RESERVATION_ARN,PAYER_NAME,BILL_PAYERACCOUNTID,USAGEACCOUNTID,ACCOUNT_TYPE,CUSTOMER_NAME,REGION,PRODUCT_NAME,
OPERATINGSYSTEM_ENGINE,INSTANCETYPE_FAMILY,SAVINGSPLAN_TYPE,LINEITEM_USAGESTARTDATE,TOTAL_NU,USED_NU,UNUSED_NU,
WASTAGE_PERCENTAGE,OFFERING_TYPE,RESERVATION_RESERVATIONARN,RESERVEDHOURS,USED_HOURS,UNUSED_HOURS,WASTAGE_COST,
RI_UTILIZATION,MYCLOUD_STARTMONTH,MYCLOUD_STARTYEAR,INSTANCETYPE,NFAPPLIED,QUANTITY,RI_ECOST
)
select
RESERVATION_ID ,
PAYER_NAME,
a.BILL_PAYERACCOUNTID,
LINKEDACCOUNTID,
FAMILY_TYPE,
CUSTOMER_NAME,
REGION,
'Redshift' as product_name,
OPERATINGSYSTEM,
INSTANCETYPE_FAMILY,
'' as SAVINGSPLAN_TYPE,
STARTDATE,
TOTAL_NU,
USED_NU,
total_nu - used_nu as unused_nu,
unused_nu/total_nu as wastage_percentage,
OFFERING_TYPE,
RESERVATION_RESERVATIONARN,
RESERVEDHOURS,
USEDHOURS as actual_used_hours,
reservedHours - actual_used_hours as unused_hours,
(reservedHours - actual_used_hours)*ri_ecost as wastage_cost,
(actual_used_hours/reservedHours)*1 as ri_utilization,
MYCLOUD_STARTMONTH,
MYCLOUD_STARTYEAR,
INSTANCETYPE_FAMILY,
NFAPPLIED,
QUANTITY,
RI_ECOST
from redshift_ri_utilization_report_hourly_temp a
left join Redshift_payer_max_date b on a.BILL_PAYERACCOUNTID = b.BILL_PAYERACCOUNTID
where 
a.STARTDATE <= b.max_date and 
reservedHours <> 0;


update ck_analytics_application_ri_wastage_hourly
set total_units =  (case
                                when (INSTANCETYPE_FAMILY ilike ('%g4ad%') or INSTANCETYPE_FAMILY ilike ('%g5%') or INSTANCETYPE_FAMILY ilike ('%g5g%') or
                                INSTANCETYPE_FAMILY ilike ('%g6%') or INSTANCETYPE_FAMILY ilike ('%g6e%') or INSTANCETYPE_FAMILY ilike ('%gr6%') or INSTANCETYPE_FAMILY ilike ('%inf1%') or INSTANCETYPE_FAMILY ilike ('%g4dn%') or INSTANCETYPE_FAMILY ilike ('%inf2%') or
                                INSTANCETYPE_FAMILY ilike ('%hpc7a%') or INSTANCETYPE_FAMILY ilike ('%p5%') or INSTANCETYPE_FAMILY ilike ('%u7i-6tb%')
                                or INSTANCETYPE_FAMILY ilike ('%u7i-8tb%'))
                                then TOTAL_NU/NFAPPLIED
                                when PRODUCT_NAME = 'EC2(RIs)' and   operatingsystem_engine like '%BYOL%'
                                then total_nu
                                when PRODUCT_NAME = 'EC2(RIs)' and   operatingsystem_engine = 'Linux/UNIX'
                                then total_nu
                                when PRODUCT_NAME = 'RDS' and operatingsystem_engine like '%BYOL%'
                                then total_nu
                                when PRODUCT_NAME = 'RDS' and operatingsystem_engine in ('MySQL', 'Aurora MySQL', 'PostgreSQL', 'Aurora PostgreSQL', 'MariaDB')
                                then total_nu
                                when PRODUCT_NAME = 'ElastiCache'
                                then total_nu
                                 when PRODUCT_NAME = 'OpenSearch'
                                then total_nu
                                when PRODUCT_NAME = 'Redshift'
                                then total_nu
                                else TOTAL_NU/NFAPPLIED
                                end ),
used_units = (case
                                when (INSTANCETYPE_FAMILY ilike ('%g4ad%') or INSTANCETYPE_FAMILY ilike ('%g5%') or INSTANCETYPE_FAMILY ilike ('%g5g%') or
                                INSTANCETYPE_FAMILY ilike ('%g6%') or INSTANCETYPE_FAMILY ilike ('%g6e%') or INSTANCETYPE_FAMILY ilike ('%gr6%') or INSTANCETYPE_FAMILY ilike ('%inf1%') or INSTANCETYPE_FAMILY ilike ('%g4dn%') or INSTANCETYPE_FAMILY ilike ('%inf2%') or
                                INSTANCETYPE_FAMILY ilike ('%hpc7a%') or INSTANCETYPE_FAMILY ilike ('%p5%') or INSTANCETYPE_FAMILY ilike ('%u7i-6tb%')
                                or INSTANCETYPE_FAMILY ilike ('%u7i-8tb%'))
                                then USED_NU/NFAPPLIED
                                   when PRODUCT_NAME = 'EC2(RIs)' and   operatingsystem_engine like '%BYOL%'
                                then USED_NU
                                when PRODUCT_NAME = 'EC2(RIs)' and   operatingsystem_engine = 'Linux/UNIX'
                                then USED_NU
                                when PRODUCT_NAME = 'RDS' and operatingsystem_engine like '%BYOL%'
                                then USED_NU
                                when PRODUCT_NAME = 'RDS' and operatingsystem_engine in ('MySQL', 'Aurora MySQL', 'PostgreSQL', 'Aurora PostgreSQL', 'MariaDB')
                                then USED_NU
                                when PRODUCT_NAME = 'ElastiCache'
                                then USED_NU
                                 when PRODUCT_NAME = 'OpenSearch'
                                then USED_NU
                                when PRODUCT_NAME = 'Redshift'
                                then USED_NU
                                else USED_NU/NFAPPLIED
                                end ),
unused_units = (case
                                when (INSTANCETYPE_FAMILY ilike ('%g4ad%') or INSTANCETYPE_FAMILY ilike ('%g5%') or INSTANCETYPE_FAMILY ilike ('%g5g%') or
                                INSTANCETYPE_FAMILY ilike ('%g6%') or INSTANCETYPE_FAMILY ilike ('%g6e%') or INSTANCETYPE_FAMILY ilike ('%gr6%') or INSTANCETYPE_FAMILY ilike ('%inf1%') or INSTANCETYPE_FAMILY ilike ('%g4dn%') or INSTANCETYPE_FAMILY ilike ('%inf2%') or
                                INSTANCETYPE_FAMILY ilike ('%hpc7a%') or INSTANCETYPE_FAMILY ilike ('%p5%') or INSTANCETYPE_FAMILY ilike ('%u7i-6tb%')
                                or INSTANCETYPE_FAMILY ilike ('%u7i-8tb%'))
                                then UNUSED_NU/NFAPPLIED
                                   when PRODUCT_NAME = 'EC2(RIs)' and   operatingsystem_engine like '%BYOL%'
                                then UNUSED_NU
                                when PRODUCT_NAME = 'EC2(RIs)' and   operatingsystem_engine = 'Linux/UNIX'
                                then UNUSED_NU
                                when PRODUCT_NAME = 'RDS' and operatingsystem_engine like '%BYOL%'
                                then UNUSED_NU
                                when PRODUCT_NAME = 'RDS' and operatingsystem_engine in ('MySQL', 'Aurora MySQL', 'PostgreSQL', 'Aurora PostgreSQL', 'MariaDB')
                                then UNUSED_NU
                                when PRODUCT_NAME = 'ElastiCache'
                                then UNUSED_NU
                                when PRODUCT_NAME = 'OpenSearch'
                                then UNUSED_NU
                                when PRODUCT_NAME = 'Redshift'
                                then UNUSED_NU
                                else UNUSED_NU/NFAPPLIED
                                end )
where mycloud_startmonth = #startmonth and mycloud_startyear = #startyear;

end;