SET VAR_START_DATE ='01-JAN-2021';
SET VAR_END_DATE = getdate();

with a_cte (shard,transdate,submitdate,headerid,lineitem,customer,name,person,custcat,standcat,submiles,calcmiles,personmiles,step,totalamt,reimbursable,currencycode) as 
            ((select 'C1' as shard,l.transactiondate,h.submitdate,h.expensereportheaderid,l.expensereportlineitemid,l.customerid,c.name, p.personid ,ec.CUSTOMER_EXPENSE_CATEGORY , ec.STANDARD_EXPENSE_CATEGORY,max (lt.miles) as submiles,max(lt.calculatedmiles) as calcmiles,max(lt.personalmiles) as personmiles,max(lt.tripstep), l.amountspent,l.isfirmpaid,l.CURRENCYCODESPENT

from "BRONZE_CR"."CR_C1_PROD_CHROME_EXPENSE"."TBL_EXPENSEREPORTLINEITEM" L 
left join  "BRONZE_CR"."CR_C1_PROD_CHROME_EXPENSE"."TBL_EXPENSEREPORTLINEITEMCOMPLIANCE" lc on lc.expensereportlineitemid = l.expensereportlineitemid
left join "BRONZE_CR"."CR_C1_PROD_CHROME_EXPENSE"."TBL_EXPENSEREPORTLINEITEMTRIP"  lt on lt.expensereportlineitemid = lc.expensereportlineitemid
left join  "SILVER_CR"."CR_PROD"."TBL_STANDARD_EXPENSE_TYPE" ec on ec.expensereportlineitemid = l.expensereportlineitemid 
left join "BRONZE_CR"."CR_C1_PROD_CHROME_EXPENSE"."TBL_EXPENSEREPORTHEADER" h on  h.EXPENSEREPORTHEADERID = l.EXPENSEREPORTHEADERID
left join "BRONZE_CR"."CR_C1_PROD_CHROME_EXPENSE"."TBL_PERSON" p on p.personid = h.personid
left join "BRONZE_CR"."CR_C1_PROD_CHROME_EXPENSE"."TBL_CUSTOMER" c on c.customerid = l.customerid
             
where h.submitdate>= $var_start_date and h.submitdate <= $var_end_date
and h._FIVETRAN_DELETED = 'FALSE' and l._FIVETRAN_DELETED = 'FALSE' and lc._FIVETRAN_DELETED = 'FALSE' and lt._FIVETRAN_DELETED = 'FALSE'
AND lower(ec.CUSTOMER_EXPENSE_CATEGORY) like '%mileage%' 

group by shard,l.transactiondate,h.submitdate,h.expensereportheaderid,l.expensereportlineitemid,l.customerid,c.name, p.personid 
             ,ec.CUSTOMER_EXPENSE_CATEGORY , ec.STANDARD_EXPENSE_CATEGORY,l.amountspent,l.isfirmpaid,l.CURRENCYCODESPENT)
             
             union 
    (select 'C2' as shard,l.transactiondate,h.submitdate,h.expensereportheaderid,l.expensereportlineitemid,l.customerid,c.name, p.personid ,ec.CUSTOMER_EXPENSE_CATEGORY , ec.STANDARD_EXPENSE_CATEGORY,max (lt.miles) as submiles,max(lt.calculatedmiles) as calcmiles,max(lt.personalmiles) as personmiles,max(lt.tripstep), l.amountspent,l.isfirmpaid,l.CURRENCYCODESPENT

from "BRONZE_CR"."CR_C2_PROD_CHROME_EXPENSE"."TBL_EXPENSEREPORTLINEITEM" L 
left join  "BRONZE_CR"."CR_C2_PROD_CHROME_EXPENSE"."TBL_EXPENSEREPORTLINEITEMCOMPLIANCE" lc on lc.expensereportlineitemid = l.expensereportlineitemid
left join "BRONZE_CR"."CR_C2_PROD_CHROME_EXPENSE"."TBL_EXPENSEREPORTLINEITEMTRIP"  lt on lt.expensereportlineitemid = lc.expensereportlineitemid
left join  "SILVER_CR"."CR_PROD"."TBL_STANDARD_EXPENSE_TYPE" ec on ec.expensereportlineitemid = l.expensereportlineitemid 
left join "BRONZE_CR"."CR_C2_PROD_CHROME_EXPENSE"."TBL_EXPENSEREPORTHEADER" h on  h.EXPENSEREPORTHEADERID = l.EXPENSEREPORTHEADERID
left join "BRONZE_CR"."CR_C2_PROD_CHROME_EXPENSE"."TBL_PERSON" p on p.personid = h.personid
left join "BRONZE_CR"."CR_C2_PROD_CHROME_EXPENSE"."TBL_CUSTOMER" c on c.customerid = l.customerid
             
where h.submitdate>= $var_start_date and h.submitdate <= $var_end_date
and h._FIVETRAN_DELETED = 'FALSE' and l._FIVETRAN_DELETED = 'FALSE' and lc._FIVETRAN_DELETED = 'FALSE' and lt._FIVETRAN_DELETED = 'FALSE'
AND lower(ec.CUSTOMER_EXPENSE_CATEGORY) like '%mileage%' 

group by shard,l.transactiondate,h.submitdate,h.expensereportheaderid,l.expensereportlineitemid,l.customerid,c.name, p.personid 
             ,ec.CUSTOMER_EXPENSE_CATEGORY , ec.STANDARD_EXPENSE_CATEGORY,l.amountspent,l.isfirmpaid,l.CURRENCYCODESPENT)
             union 
             
             (select 'C7' as shard,l.transactiondate,h.submitdate,h.expensereportheaderid,l.expensereportlineitemid,l.customerid,c.name, p.personid ,ec.CUSTOMER_EXPENSE_CATEGORY , ec.STANDARD_EXPENSE_CATEGORY,max (lt.miles) as submiles,max(lt.calculatedmiles) as calcmiles,max(lt.personalmiles) as personmiles,max(lt.tripstep), l.amountspent,l.isfirmpaid,l.CURRENCYCODESPENT

from "BRONZE_CR"."CR_C7_PROD_CHROME_EXPENSE"."TBL_EXPENSEREPORTLINEITEM" L 
left join  "BRONZE_CR"."CR_C7_PROD_CHROME_EXPENSE"."TBL_EXPENSEREPORTLINEITEMCOMPLIANCE" lc on lc.expensereportlineitemid = l.expensereportlineitemid
left join "BRONZE_CR"."CR_C7_PROD_CHROME_EXPENSE"."TBL_EXPENSEREPORTLINEITEMTRIP"  lt on lt.expensereportlineitemid = lc.expensereportlineitemid
left join  "SILVER_CR"."CR_PROD"."TBL_STANDARD_EXPENSE_TYPE" ec on ec.expensereportlineitemid = l.expensereportlineitemid 
left join "BRONZE_CR"."CR_C7_PROD_CHROME_EXPENSE"."TBL_EXPENSEREPORTHEADER" h on  h.EXPENSEREPORTHEADERID = l.EXPENSEREPORTHEADERID
left join "BRONZE_CR"."CR_C7_PROD_CHROME_EXPENSE"."TBL_PERSON" p on p.personid = h.personid
left join "BRONZE_CR"."CR_C7_PROD_CHROME_EXPENSE"."TBL_CUSTOMER" c on c.customerid = l.customerid
             
where h.submitdate>= $var_start_date and h.submitdate <= $var_end_date
and h._FIVETRAN_DELETED = 'FALSE' and l._FIVETRAN_DELETED = 'FALSE' and lc._FIVETRAN_DELETED = 'FALSE' and lt._FIVETRAN_DELETED = 'FALSE'
AND lower(ec.CUSTOMER_EXPENSE_CATEGORY) like '%mileage%' 

group by shard,l.transactiondate,h.submitdate,h.expensereportheaderid,l.expensereportlineitemid,l.customerid,c.name, p.personid 
             ,ec.CUSTOMER_EXPENSE_CATEGORY , ec.STANDARD_EXPENSE_CATEGORY,l.amountspent,l.isfirmpaid,l.CURRENCYCODESPENT)),

c_cte (CURRENCYCODETO, CURRENCYCODEFROM, DATEEFF, DATEEND, EXRATE)
as
(
select distinct c.CURRENCYCODE as CURRENCYCODETO, cpta.DISBURSEMENTCURRENCYCODE as CURRENCYCODEFROM, cpta.DATEEFFECTIVE as DATEEFF, cpta.DATEEND as DATEEND, cpta.EXCHANGERATETOFIRMCURRENCY AS EXRATE
from "BRONZE_CR"."CR_C1_PROD_CHROME_EXPENSE"."TBL_CURRENCYPTA" cpta
, "BRONZE_CR"."CR_C1_PROD_CHROME_EXPENSE"."TBL_CUSTOMER" c
where cpta.CUSTOMERID = c.CUSTOMERID
and c.CURRENCYCODE = 'USD'
and cpta._FIVETRAN_DELETED = 'FALSE'
)
            
select a_cte.shard as shardlocation
, year(a_cte.submitdate) as year
, month (a_cte.submitdate) as month
--, a_cte.transdate as txndate
--, a_cte.lineitem as lineitemid
, a_cte.customer as customerid
, a_cte.name as customername
, a_cte.person as personid
, a_cte.custcat as customcat
, a_cte.standcat as standardcat
, case when a_cte.customer in (683, 613, 635, 559, 666, 704, 710, 718, 949, 939, 942, 335, 372, 370, 408, 430, 647, 694, 938, 945, 916, 946, 1001, 332, 350, 385, 723, 433, 450,
                             460, 470, 472, 463, 550, 538, 541, 545, 611, 575, 608, 593, 600, 633, 686, 668, 682, 680, 546, 615, 918, 720, 743, 906, 373, 984, 294, 330, 194, 
                             449, 442, 506, 571, 577, 579, 498, 574, 573, 607, 612, 629, 477, 699, 701, 702, 696, 667, 907, 705, 713, 535, 563, 581, 619, 582, 602, 651, 664, 
                             656, 675, 697, 2098, 662, 649, 690, 614, 618, 636, 676, 621, 717, 714, 727, 570, 308, 310, 468, 616, 609, 730, 936, 937, 940, 941, 944, 985, 979, 
                             258, 242, 353, 943, 411, 409, 427, 993, 336, 188, 517, 537, 561, 568, 565, 586, 623, 605, 661, 638, 658, 657, 663, 910, 520, 627, 695, 931, 915,
                             302, 380, 341, 446, 459, 499, 576, 585, 597, 591, 596, 617, 641, 622, 648, 665, 679, 689, 735, 738, 935, 643, 640, 569, 599, 659, 692, 740, 706, 
                             724, 531, 528, 610, 644, 731, 639, 583, 669, 930, 953, 989, 42, 587, 654) then 'Y' else 'N' end as RequiresGoogleMaps
, sum(a_cte.submiles) as submitted_miles
, sum(a_cte.calcmiles) as calculated_miles
, sum (a_cte.personmiles) as personal_miles
, max(a_cte.step) as maxtripstep
, count (a_cte.headerid) as reportcount 
, case when a_cte.reimbursable = 1 then 'Y' else 'N' end as FirmPaid
, sum(case when a_cte.CURRENCYCODE = 'USD' then a_cte.totalamt else a_cte.totalamt*c_cte.EXRATE end) as usd_amt
            
from a_cte
left join c_cte on c_cte.CURRENCYCODEFROM = DECODE(a_cte.CURRENCYCODE,'RMB','CNY','NTD','TWD','TRL','TRY',a_cte.CURRENCYCODE)
and c_cte.DATEEFF <= a_cte.transdate
and (c_cte.DATEEND > a_cte.transdate or c_cte.DATEEND is null)

group by shardlocation, year, month,customerid,customername,personid,customcat,standardcat,a_cte.reimbursable  having usd_amt >0 order by shardlocation,year,month,customerid,customername,personid,customcat,standardcat
