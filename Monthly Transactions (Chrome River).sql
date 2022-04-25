SET VAR_START_DATE ='01-JAN-2021';
SET VAR_END_DATE = GETDATE();


create OR REPLACE table "JBML"."PUBLIC"."MONTHLYTXN_CR_US" AS 

with a_cte (TRANSDATE,EXPENSEID,TOTALAMT,CURRENCYCODE) as
(SELECT T.TRANSACTIONDATE AS TRANSDATE, T.EXPENSETRANSACTIONID AS TRANSID, T.AMOUNTSPENT AS TOTALAMT, T.CURRENCYCODESPENT AS CURRENCYCODE

FROM  "BRONZE_CR"."CR_C1_PROD_CHROME_EXPENSE"."TBL_EXPENSETRANSACTION" T, "BRONZE_CR"."CR_C1_PROD_CHROME_EXPENSE"."TBL_FEED" F 

WHERE T.FEEDID = F.FEEDID 
AND T.TRANSACTIONDATE>= $var_start_date and T.TRANSACTIONDATE<= $var_end_date
AND T._FIVETRAN_DELETED = 'FALSE'
AND (T.TYPE = 'DEF' AND T.ISFIRMPAID = 1) AND TOTALAMT>0

UNION ALL
 
SELECT T.TRANSACTIONDATE AS TRANSDATE, T.EXPENSETRANSACTIONID AS TRANSID, T.AMOUNTSPENT AS TOTALAMT, T.CURRENCYCODESPENT AS CURRENCYCODE

FROM  "BRONZE_CR"."CR_C2_PROD_CHROME_EXPENSE"."TBL_EXPENSETRANSACTION" T, "BRONZE_CR"."CR_C1_PROD_CHROME_EXPENSE"."TBL_FEED" F 

WHERE T.FEEDID = F.FEEDID 
AND T.TRANSACTIONDATE>= $var_start_date and T.TRANSACTIONDATE<= $var_end_date
AND T._FIVETRAN_DELETED = 'FALSE'
AND (T.TYPE = 'DEF' AND T.ISFIRMPAID = 1) AND TOTALAMT>0

UNION ALL
 
SELECT T.TRANSACTIONDATE AS TRANSDATE, T.EXPENSETRANSACTIONID AS TRANSID, T.AMOUNTSPENT AS TOTALAMT, T.CURRENCYCODESPENT AS CURRENCYCODE

FROM  "BRONZE_CR"."CR_C7_PROD_CHROME_EXPENSE"."TBL_EXPENSETRANSACTION" T, "BRONZE_CR"."CR_C1_PROD_CHROME_EXPENSE"."TBL_FEED" F 

WHERE T.FEEDID = F.FEEDID 
AND T.TRANSACTIONDATE>= $var_start_date and T.TRANSACTIONDATE<= $var_end_date
AND T._FIVETRAN_DELETED = 'FALSE'
AND (T.TYPE = 'DEF' AND T.ISFIRMPAID = 1) AND TOTALAMT>0
)

,
c_cte (CURRENCYCODETO, CURRENCYCODEFROM, DATEEFF, DATEEND, EXRATE)
as
(select distinct c.CURRENCYCODE as CURRENCYCODETO, cpta.DISBURSEMENTCURRENCYCODE as CURRENCYCODEFROM, cpta.DATEEFFECTIVE as DATEEFF, cpta.DATEEND as DATEEND, cpta.EXCHANGERATETOFIRMCURRENCY AS EXRATE
from "BRONZE_CR"."CR_C1_PROD_CHROME_EXPENSE"."TBL_CURRENCYPTA" cpta
, "BRONZE_CR"."CR_C1_PROD_CHROME_EXPENSE"."TBL_CUSTOMER" c
where cpta.CUSTOMERID = c.CUSTOMERID
and c.CURRENCYCODE = 'USD'
and cpta._FIVETRAN_DELETED = 'FALSE'
)
SELECT  
 YEAR (a_cte.TRANSDATE) as YEAR
, MONTH (a_cte.TRANSDATE) as MONTH
, a_cte.EXPENSEID AS EXPENSEID
, CASE WHEN SUM(case when a_cte.CURRENCYCODE = 'USD' then a_cte.TOTALAMT else a_cte.TOTALAMT*c_cte.EXRATE end) BETWEEN 25 AND 75 THEN '$25 -$75 EXPENSE'  ELSE 'OTHER' END AS EXPENSERANGE

from a_cte
left join c_cte on c_cte.CURRENCYCODEFROM = a_cte.CURRENCYCODE 
and c_cte.DATEEFF < a_cte.TRANSDATE 
and c_cte.DATEEND > a_cte.TRANSDATE 

WHERE TRANSDATE>= $var_start_date and TRANSDATE<= $var_end_date
group by YEAR,MONTH,EXPENSEID
ORDER BY YEAR, MONTH;

SELECT YEAR, MONTH, EXPENSERANGE, COUNT(EXPENSEID) AS EXPENSECOUNT FROM "JBML"."PUBLIC"."MONTHLYTXN_CR_US" GROUP BY YEAR, MONTH, EXPENSERANGE ORDER BY YEAR, MONTH, EXPENSERANGE
 