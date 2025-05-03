CREATE OR REPLACE FUNCTION public.expiryfn(acc date, date date, duration integer, extendvalidity boolean, voiding date) RETURNS date
    LANGUAGE sql
    AS $$
SELECT
  LEAST(
    CASE
        WHEN duration = 0 THEN 'infinity' :: DATE
        WHEN acc IS NULL OR extendvalidity IS FALSE OR acc <= date
        THEN date + (duration * INTERVAL '1 month')
        ELSE acc + (duration * INTERVAL '1 month')
    END,
    voiding
  );
$$;
