CREATE TABLE IF NOT EXISTS public.trainingtypes_defs (
    ttdf_pk serial PRIMARY KEY,
    ttdf_trty_fk integer NOT NULL,
    ttdf_effective_from date NOT NULL DEFAULT '-infinity'::date,
    ttdf_presenceonly boolean DEFAULT false NOT NULL,
    ttdf_extendvalidity boolean DEFAULT false NOT NULL,
    CONSTRAINT ttdf_trty_fk FOREIGN KEY (ttdf_trty_fk) REFERENCES public.trainingtypes(trty_pk) ON DELETE CASCADE,
    CONSTRAINT unique_trty_effective_from UNIQUE (ttdf_trty_fk, ttdf_effective_from)
);
COMMENT ON COLUMN public.trainingtypes_defs.ttdf_extendvalidity IS 'Whether the certificates granted by that training type should merely be renewed for the duration set up, or their validity be *extended* by that amount.';

INSERT INTO public.trainingtypes_defs (
    ttdf_trty_fk, ttdf_effective_from, ttdf_presenceonly, ttdf_extendvalidity
)
SELECT
    trty_pk, '-infinity'::date, trty_presenceonly, trty_extendvalidity
FROM public.trainingtypes
ON CONFLICT ON CONSTRAINT unique_trty_effective_from DO NOTHING;

ALTER TABLE public.trainingtypes_certificates
ADD COLUMN IF NOT EXISTS ttce_ttdf_fk integer;

UPDATE public.trainingtypes_certificates c
SET ttce_ttdf_fk = (
    SELECT ttdf_pk
    FROM public.trainingtypes_defs d
    WHERE d.ttdf_trty_fk = c.ttce_trty_fk
    AND d.ttdf_effective_from = '-infinity'::date
);

ALTER TABLE public.trainingtypes_certificates
ALTER COLUMN ttce_ttdf_fk SET NOT NULL,
ADD CONSTRAINT ttce_ttdf_fk FOREIGN KEY (ttce_ttdf_fk) REFERENCES public.trainingtypes_defs(ttdf_pk) ON DELETE CASCADE;

ALTER TABLE public.trainingtypes_certificates
DROP COLUMN IF EXISTS ttce_trty_fk;

ALTER TABLE public.trainingtypes_certificates
ADD CONSTRAINT fk_combination_uniq UNIQUE (ttce_ttdf_fk, ttce_cert_fk);

ALTER TABLE public.trainingtypes
DROP COLUMN IF EXISTS trty_presenceonly,
DROP COLUMN IF EXISTS trty_extendvalidity;

CREATE INDEX IF NOT EXISTS idx_trainingtypes_defs_trty_effective_from
ON public.trainingtypes_defs (ttdf_trty_fk, ttdf_effective_from DESC);
