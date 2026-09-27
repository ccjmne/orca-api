ALTER TABLE certificates
  ADD COLUMN cert_trty_fk integer
  UNIQUE
  REFERENCES trainingtypes(trty_pk)
  ON DELETE CASCADE,
  ADD CONSTRAINT certificates_cert_short_key UNIQUE (cert_short);
