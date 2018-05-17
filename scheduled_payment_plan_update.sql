-- grant python permission to test the udf
grant USAGE on language plpythonu to kduponte;

-- UDF to create the full history versions of payments due for IL Mods
create or replace function scratch.f_il_mod_full_history (original varchar(1000), most_recent varchar(1000), il_mod_date timestamp)
    returns varchar(1000)

    stable as $$

        import json
        import datetime

        original_dict = json.loads(original)
        most_recent_dict = json.loads(most_recent)

        if il_mod_date == None:
          return original
        else:
          full_dict = {k:v for k,v in original_dict.iteritems() if datetime.datetime.strptime(k, "%a %b %d %H:%M:%S PDT %Y") <= il_mod_date}
          full_dict.update(most_recent_dict)
          return json.dumps(full_dict)

    $$ language plpythonu;


-- Selecting max version of loan_audit, which would be used for most_recent
-- and also have details of the loan mod version
with mv as (
  select id, max(version) as max_version
  from loan_audit
  group by 1
),

-- Create 3 versions per loan:
-- 1) original - the loan details at origination
-- 2) most_recent - most recent loan details
-- 3) full - full history of loan payments
t as (
      SELECT
        haspaymentplan,
        il_mod.il_mod_date,
        original.paymentsdue    AS original_paymentsdue,
        original.numpayments    AS original_numpayments,
        la.paymentsdue          AS most_recent_paymentsdue,
        la.numpayments          AS most_recent_numpayments,
        scratch.f_il_mod_full_history(original.paymentsdue, la.paymentsdue, il_mod.il_mod_date) AS full_paymentsdue

  -- filtering on max/most_recent version of loan_audit and using this one sample user 40207
  -- Also left joining the original version details and loan_mod date
      FROM (SELECT
              loan_audit.id,
              loan_audit.user_id,
              loan_audit.paymentsdue,
              loan_audit.numpayments,
              loan_audit.haspaymentplan
            FROM loan_audit
              INNER JOIN mv ON mv.id = loan_audit.id AND mv.max_version = loan_audit.version
            WHERE loan_audit.user_id = 40207
                  AND loan_audit.dtype IN ('InstallmentLoan', 'PrimeLoan')) la
        LEFT JOIN (
                    SELECT
                      id,
                      paymentsdue,
                      numpayments,
                      lastedited as origination_date
                    FROM loan_audit
                    WHERE version = 0
                  ) original ON original.id = la.id
        LEFT JOIN (
                    SELECT
                      id,
                      MIN(lastedited) as il_mod_date
                    FROM loan_audit
                    WHERE haspaymentplan = TRUE
                    GROUP BY 1
                  ) il_mod ON il_mod.id = la.id
  )

  -- Comparing original, most_recent, and full
  select
    haspaymentplan,
    il_mod_date,
    original_paymentsdue,
    most_recent_paymentsdue,
    full_paymentsdue,
    original_numpayments,
    most_recent_numpayments,
    REGEXP_COUNT (full_paymentsdue, '"[[:alnum:][:space:]:]+": "[[:digit:]E.]+"') as full_numpayments
from t
limit 100;


select *
from svl_udf_log;

select *
from loan_audit
WHERE loan_audit.user_id = 40207;

