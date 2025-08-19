ALTER TABLE public.trainingtypes_defs
    ADD ttdf_certified bool DEFAULT false NOT NULL;
COMMENT ON COLUMN public.trainingtypes_defs.ttdf_certified
    IS 'Whether the training type corresponds to an official certification.';
ALTER TABLE public.trainingtypes_defs
    ADD CONSTRAINT trainingtypes_defs_check_certified_not_presenceonly
    CHECK (NOT (ttdf_presenceonly AND ttdf_certified));
