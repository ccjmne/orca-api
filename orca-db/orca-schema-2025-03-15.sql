--
-- PostgreSQL database dump
--

-- Dumped from database version 16.4
-- Dumped by pg_dump version 16.8

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: public; Type: SCHEMA; Schema: -; Owner: orcadb_root
--

-- *not* creating schema, since initdb creates it


ALTER SCHEMA public OWNER TO orcadb_root;

--
-- Name: f_concat_ws(text[]); Type: FUNCTION; Schema: public; Owner: orcadb_root
--

CREATE FUNCTION public.f_concat_ws(VARIADIC text[]) RETURNS text
    LANGUAGE sql IMMUTABLE
    AS $_$
SELECT pg_catalog.concat_ws(' '::text, VARIADIC $1)  -- schema-qualify function and dictionary
$_$;


ALTER FUNCTION public.f_concat_ws(VARIADIC text[]) OWNER TO orcadb_root;

--
-- Name: f_unaccent(text); Type: FUNCTION; Schema: public; Owner: orcadb_root
--

CREATE FUNCTION public.f_unaccent(text) RETURNS text
    LANGUAGE sql IMMUTABLE
    AS $_$
SELECT public.unaccent('public.unaccent', $1)  -- schema-qualify function and dictionary
$_$;


ALTER FUNCTION public.f_unaccent(text) OWNER TO orcadb_root;

--
-- Name: make_into_serial(text, text); Type: FUNCTION; Schema: public; Owner: orcadb_root
--

CREATE FUNCTION public.make_into_serial(table_name text, column_name text) RETURNS integer
    LANGUAGE plpgsql
    AS $$
DECLARE
    start_with INTEGER;
    sequence_name TEXT;
BEGIN
    sequence_name := table_name || '_' || column_name || '_seq';
    EXECUTE 'SELECT coalesce(max(' || column_name || '), 0) + 1 FROM ' || table_name
            INTO start_with;
    EXECUTE 'CREATE SEQUENCE ' || sequence_name ||
            ' START WITH ' || start_with ||
            ' OWNED BY ' || table_name || '.' || column_name;
    EXECUTE 'ALTER TABLE ' || table_name || ' ALTER COLUMN ' || column_name ||
            ' SET DEFAULT nextVal(''' || sequence_name || ''')';
    RETURN start_with;
END;
$$;


ALTER FUNCTION public.make_into_serial(table_name text, column_name text) OWNER TO orcadb_root;

--
-- Name: cert_order_seq; Type: SEQUENCE; Schema: public; Owner: orcadb_root
--

CREATE SEQUENCE public.cert_order_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.cert_order_seq OWNER TO orcadb_root;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: certificates; Type: TABLE; Schema: public; Owner: orcadb_root
--

CREATE TABLE public.certificates (
    cert_pk integer NOT NULL,
    cert_name character varying(128) NOT NULL,
    cert_target integer NOT NULL,
    cert_permanentonly boolean DEFAULT false NOT NULL,
    cert_short character varying(32) NOT NULL,
    cert_order integer DEFAULT nextval('public.cert_order_seq'::regclass) NOT NULL
);


ALTER TABLE public.certificates OWNER TO orcadb_root;

--
-- Name: certificates_cert_pk_seq; Type: SEQUENCE; Schema: public; Owner: orcadb_root
--

CREATE SEQUENCE public.certificates_cert_pk_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.certificates_cert_pk_seq OWNER TO orcadb_root;

--
-- Name: certificates_cert_pk_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: orcadb_root
--

ALTER SEQUENCE public.certificates_cert_pk_seq OWNED BY public.certificates.cert_pk;


--
-- Name: client; Type: TABLE; Schema: public; Owner: orcadb_root
--

CREATE TABLE public.client (
    clnt_id character varying NOT NULL,
    clnt_name character varying NOT NULL,
    clnt_mailto character varying NOT NULL,
    clnt_logo character varying NOT NULL,
    clnt_livechat boolean DEFAULT false NOT NULL
);


ALTER TABLE public.client OWNER TO orcadb_root;

--
-- Name: configs; Type: TABLE; Schema: public; Owner: orcadb_root
--

CREATE TABLE public.configs (
    conf_pk integer NOT NULL,
    conf_type character varying(16) NOT NULL,
    conf_name character varying NOT NULL,
    conf_data text NOT NULL,
    CONSTRAINT conf_type CHECK (((conf_type)::text = ANY (ARRAY[('import-employees'::character varying)::text, ('import-sites'::character varying)::text, ('pdf-site'::character varying)::text])))
);


ALTER TABLE public.configs OWNER TO orcadb_root;

--
-- Name: configs_conf_pk_seq; Type: SEQUENCE; Schema: public; Owner: orcadb_root
--

CREATE SEQUENCE public.configs_conf_pk_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.configs_conf_pk_seq OWNER TO orcadb_root;

--
-- Name: configs_conf_pk_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: orcadb_root
--

ALTER SEQUENCE public.configs_conf_pk_seq OWNED BY public.configs.conf_pk;


--
-- Name: employees; Type: TABLE; Schema: public; Owner: orcadb_root
--

CREATE TABLE public.employees (
    empl_pk integer NOT NULL,
    empl_firstname character varying NOT NULL,
    empl_surname character varying NOT NULL,
    empl_dob date NOT NULL,
    empl_permanent boolean NOT NULL,
    empl_address character varying,
    empl_notes character varying,
    empl_gender boolean DEFAULT false NOT NULL,
    empl_external_id character varying,
    empl_birthname text,
    empl_birthplace_city text,
    empl_birthplace_country text
);


ALTER TABLE public.employees OWNER TO orcadb_root;

--
-- Name: employees_empl_pk_seq; Type: SEQUENCE; Schema: public; Owner: orcadb_root
--

CREATE SEQUENCE public.employees_empl_pk_seq
    START WITH 12841
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.employees_empl_pk_seq OWNER TO orcadb_root;

--
-- Name: employees_empl_pk_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: orcadb_root
--

ALTER SEQUENCE public.employees_empl_pk_seq OWNED BY public.employees.empl_pk;


--
-- Name: employees_voidings; Type: TABLE; Schema: public; Owner: orcadb_root
--

CREATE TABLE public.employees_voidings (
    emvo_empl_fk integer NOT NULL,
    emvo_cert_fk integer NOT NULL,
    emvo_date date NOT NULL,
    emvo_reason character varying
);


ALTER TABLE public.employees_voidings OWNER TO orcadb_root;

--
-- Name: sites; Type: TABLE; Schema: public; Owner: orcadb_root
--

CREATE TABLE public.sites (
    site_pk integer NOT NULL,
    site_name character varying NOT NULL,
    site_notes character varying,
    site_address character varying,
    site_external_id character varying NOT NULL
);


ALTER TABLE public.sites OWNER TO orcadb_root;

--
-- Name: sites_employees; Type: TABLE; Schema: public; Owner: orcadb_root
--

CREATE TABLE public.sites_employees (
    siem_empl_fk integer NOT NULL,
    siem_site_fk integer NOT NULL,
    siem_updt_fk integer NOT NULL
);


ALTER TABLE public.sites_employees OWNER TO orcadb_root;

--
-- Name: sites_site_pk_seq; Type: SEQUENCE; Schema: public; Owner: orcadb_root
--

CREATE SEQUENCE public.sites_site_pk_seq
    START WITH 190
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.sites_site_pk_seq OWNER TO orcadb_root;

--
-- Name: sites_site_pk_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: orcadb_root
--

ALTER SEQUENCE public.sites_site_pk_seq OWNED BY public.sites.site_pk;


--
-- Name: sites_tags; Type: TABLE; Schema: public; Owner: orcadb_root
--

CREATE TABLE public.sites_tags (
    sita_site_fk integer NOT NULL,
    sita_tags_fk integer NOT NULL,
    sita_value character varying NOT NULL
);


ALTER TABLE public.sites_tags OWNER TO orcadb_root;

--
-- Name: tags_order_seq; Type: SEQUENCE; Schema: public; Owner: orcadb_root
--

CREATE SEQUENCE public.tags_order_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.tags_order_seq OWNER TO orcadb_root;

--
-- Name: tags; Type: TABLE; Schema: public; Owner: orcadb_root
--

CREATE TABLE public.tags (
    tags_pk integer NOT NULL,
    tags_name character varying NOT NULL,
    tags_type character(1) DEFAULT 's'::bpchar NOT NULL,
    tags_short character varying NOT NULL,
    tags_hex_colour character varying(7) DEFAULT '#C71585'::character varying NOT NULL,
    tags_order integer DEFAULT nextval('public.tags_order_seq'::regclass) NOT NULL,
    CONSTRAINT tags_type CHECK ((tags_type = ANY (ARRAY['s'::bpchar, 'b'::bpchar])))
);


ALTER TABLE public.tags OWNER TO orcadb_root;

--
-- Name: tags_tags_pk_seq; Type: SEQUENCE; Schema: public; Owner: orcadb_root
--

CREATE SEQUENCE public.tags_tags_pk_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.tags_tags_pk_seq OWNER TO orcadb_root;

--
-- Name: tags_tags_pk_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: orcadb_root
--

ALTER SEQUENCE public.tags_tags_pk_seq OWNED BY public.tags.tags_pk;


--
-- Name: trainerprofiles; Type: TABLE; Schema: public; Owner: orcadb_root
--

CREATE TABLE public.trainerprofiles (
    trpr_id character varying(32) NOT NULL,
    trpr_pk integer NOT NULL,
    trpr_onlyown boolean DEFAULT false NOT NULL
);


ALTER TABLE public.trainerprofiles OWNER TO orcadb_root;

--
-- Name: trainerprofiles_trainingtypes; Type: TABLE; Schema: public; Owner: orcadb_root
--

CREATE TABLE public.trainerprofiles_trainingtypes (
    tptt_trpr_fk integer NOT NULL,
    tptt_trty_fk integer NOT NULL
);


ALTER TABLE public.trainerprofiles_trainingtypes OWNER TO orcadb_root;

--
-- Name: trainerprofiles_trpr_pk2_seq; Type: SEQUENCE; Schema: public; Owner: orcadb_root
--

CREATE SEQUENCE public.trainerprofiles_trpr_pk2_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.trainerprofiles_trpr_pk2_seq OWNER TO orcadb_root;

--
-- Name: trainerprofiles_trpr_pk2_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: orcadb_root
--

ALTER SEQUENCE public.trainerprofiles_trpr_pk2_seq OWNED BY public.trainerprofiles.trpr_pk;


--
-- Name: trainings; Type: TABLE; Schema: public; Owner: orcadb_root
--

CREATE TABLE public.trainings (
    trng_pk integer NOT NULL,
    trng_date date NOT NULL,
    trng_trty_fk integer NOT NULL,
    trng_outcome character varying(32) DEFAULT 'SCHEDULED'::character varying NOT NULL,
    trng_start date,
    trng_comment character varying(256),
    CONSTRAINT "outcome value" CHECK (((trng_outcome)::text = ANY (ARRAY[('CANCELLED'::character varying)::text, ('COMPLETED'::character varying)::text, ('SCHEDULED'::character varying)::text]))),
    CONSTRAINT trng_start_before_date CHECK ((trng_start <= trng_date))
);


ALTER TABLE public.trainings OWNER TO orcadb_root;

--
-- Name: trainings_employees; Type: TABLE; Schema: public; Owner: orcadb_root
--

CREATE TABLE public.trainings_employees (
    trem_pk integer NOT NULL,
    trem_trng_fk integer NOT NULL,
    trem_outcome character varying DEFAULT 'PENDING'::character varying NOT NULL,
    trem_comment character varying(128),
    trem_empl_fk integer NOT NULL,
    CONSTRAINT "outcome value" CHECK (((trem_outcome)::text = ANY (ARRAY[('CANCELLED'::character varying)::text, ('FLUNKED'::character varying)::text, ('MISSING'::character varying)::text, ('PENDING'::character varying)::text, ('VALIDATED'::character varying)::text])))
);


ALTER TABLE public.trainings_employees OWNER TO orcadb_root;

--
-- Name: trainings_employees_trem_pk_seq; Type: SEQUENCE; Schema: public; Owner: orcadb_root
--

CREATE SEQUENCE public.trainings_employees_trem_pk_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.trainings_employees_trem_pk_seq OWNER TO orcadb_root;

--
-- Name: trainings_employees_trem_pk_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: orcadb_root
--

ALTER SEQUENCE public.trainings_employees_trem_pk_seq OWNED BY public.trainings_employees.trem_pk;


--
-- Name: trainings_trainers; Type: TABLE; Schema: public; Owner: orcadb_root
--

CREATE TABLE public.trainings_trainers (
    trtr_trng_fk integer NOT NULL,
    trtr_empl_fk integer NOT NULL
);


ALTER TABLE public.trainings_trainers OWNER TO orcadb_root;

--
-- Name: trainings_trng_pk_seq; Type: SEQUENCE; Schema: public; Owner: orcadb_root
--

CREATE SEQUENCE public.trainings_trng_pk_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.trainings_trng_pk_seq OWNER TO orcadb_root;

--
-- Name: trainings_trng_pk_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: orcadb_root
--

ALTER SEQUENCE public.trainings_trng_pk_seq OWNED BY public.trainings.trng_pk;


--
-- Name: trty_order_seq; Type: SEQUENCE; Schema: public; Owner: orcadb_root
--

CREATE SEQUENCE public.trty_order_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.trty_order_seq OWNER TO orcadb_root;

--
-- Name: trainingtypes; Type: TABLE; Schema: public; Owner: orcadb_root
--

CREATE TABLE public.trainingtypes (
    trty_pk integer NOT NULL,
    trty_name character varying(128) NOT NULL,
    trty_order integer DEFAULT nextval('public.trty_order_seq'::regclass) NOT NULL,
    trty_presenceonly boolean DEFAULT false NOT NULL
);


ALTER TABLE public.trainingtypes OWNER TO orcadb_root;

--
-- Name: trainingtypes_certificates; Type: TABLE; Schema: public; Owner: orcadb_root
--

CREATE TABLE public.trainingtypes_certificates (
    ttce_trty_fk integer NOT NULL,
    ttce_cert_fk integer NOT NULL,
    ttce_duration integer DEFAULT 0 NOT NULL
);


ALTER TABLE public.trainingtypes_certificates OWNER TO orcadb_root;

--
-- Name: trainingtypes_trty_pk_seq; Type: SEQUENCE; Schema: public; Owner: orcadb_root
--

CREATE SEQUENCE public.trainingtypes_trty_pk_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.trainingtypes_trty_pk_seq OWNER TO orcadb_root;

--
-- Name: trainingtypes_trty_pk_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: orcadb_root
--

ALTER SEQUENCE public.trainingtypes_trty_pk_seq OWNED BY public.trainingtypes.trty_pk;


--
-- Name: updates; Type: TABLE; Schema: public; Owner: orcadb_root
--

CREATE TABLE public.updates (
    updt_pk integer NOT NULL,
    updt_date date NOT NULL
);


ALTER TABLE public.updates OWNER TO orcadb_root;

--
-- Name: updates_updt_pk_seq; Type: SEQUENCE; Schema: public; Owner: orcadb_root
--

CREATE SEQUENCE public.updates_updt_pk_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.updates_updt_pk_seq OWNER TO orcadb_root;

--
-- Name: updates_updt_pk_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: orcadb_root
--

ALTER SEQUENCE public.updates_updt_pk_seq OWNED BY public.updates.updt_pk;


--
-- Name: users; Type: TABLE; Schema: public; Owner: orcadb_root
--

CREATE TABLE public.users (
    user_id character varying(32) NOT NULL,
    user_pwd character varying(32) NOT NULL,
    user_type character varying(16) NOT NULL,
    user_empl_fk integer,
    user_site_fk integer,
    user_newspull_timestamp timestamp with time zone,
    user_newspull_version character varying(16),
    user_config jsonb DEFAULT '{}'::jsonb NOT NULL,
    CONSTRAINT user_empl_fk_notnull CHECK ((((user_type)::text <> 'employee'::text) OR (user_empl_fk IS NOT NULL))),
    CONSTRAINT user_site_fk_notnull CHECK ((((user_type)::text <> 'site'::text) OR (user_site_fk IS NOT NULL))),
    CONSTRAINT user_type CHECK (((user_type)::text = ANY (ARRAY[('employee'::character varying)::text, ('site'::character varying)::text, ('department'::character varying)::text])))
);


ALTER TABLE public.users OWNER TO orcadb_root;

--
-- Name: COLUMN users.user_newspull_version; Type: COMMENT; Schema: public; Owner: orcadb_root
--

COMMENT ON COLUMN public.users.user_newspull_version IS 'Follows Semantic Versioning 2.0.0';


--
-- Name: users_certificates; Type: TABLE; Schema: public; Owner: orcadb_root
--

CREATE TABLE public.users_certificates (
    usce_user_fk character varying(32) NOT NULL,
    usce_cert_fk integer
);


ALTER TABLE public.users_certificates OWNER TO orcadb_root;

--
-- Name: users_roles; Type: TABLE; Schema: public; Owner: orcadb_root
--

CREATE TABLE public.users_roles (
    user_id character varying(32) NOT NULL,
    usro_type character varying(16) NOT NULL,
    usro_level integer,
    usro_trpr_fk integer,
    CONSTRAINT usro_level_value CHECK ((((usro_type)::text <> ALL (ARRAY[('access'::character varying)::text, ('admin'::character varying)::text])) OR ((usro_level >= 1) AND (usro_level <= 4)))),
    CONSTRAINT usro_trpr_notnull CHECK ((((usro_type)::text <> 'trainer'::text) OR (usro_trpr_fk IS NOT NULL))),
    CONSTRAINT usro_type CHECK (((usro_type)::text = ANY (ARRAY[('user'::character varying)::text, ('access'::character varying)::text, ('trainer'::character varying)::text, ('admin'::character varying)::text])))
);


ALTER TABLE public.users_roles OWNER TO orcadb_root;

--
-- Name: certificates cert_pk; Type: DEFAULT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.certificates ALTER COLUMN cert_pk SET DEFAULT nextval('public.certificates_cert_pk_seq'::regclass);


--
-- Name: configs conf_pk; Type: DEFAULT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.configs ALTER COLUMN conf_pk SET DEFAULT nextval('public.configs_conf_pk_seq'::regclass);


--
-- Name: employees empl_pk; Type: DEFAULT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.employees ALTER COLUMN empl_pk SET DEFAULT nextval('public.employees_empl_pk_seq'::regclass);


--
-- Name: sites site_pk; Type: DEFAULT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.sites ALTER COLUMN site_pk SET DEFAULT nextval('public.sites_site_pk_seq'::regclass);


--
-- Name: tags tags_pk; Type: DEFAULT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.tags ALTER COLUMN tags_pk SET DEFAULT nextval('public.tags_tags_pk_seq'::regclass);


--
-- Name: trainerprofiles trpr_pk; Type: DEFAULT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.trainerprofiles ALTER COLUMN trpr_pk SET DEFAULT nextval('public.trainerprofiles_trpr_pk2_seq'::regclass);


--
-- Name: trainings trng_pk; Type: DEFAULT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.trainings ALTER COLUMN trng_pk SET DEFAULT nextval('public.trainings_trng_pk_seq'::regclass);


--
-- Name: trainings_employees trem_pk; Type: DEFAULT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.trainings_employees ALTER COLUMN trem_pk SET DEFAULT nextval('public.trainings_employees_trem_pk_seq'::regclass);


--
-- Name: trainingtypes trty_pk; Type: DEFAULT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.trainingtypes ALTER COLUMN trty_pk SET DEFAULT nextval('public.trainingtypes_trty_pk_seq'::regclass);


--
-- Name: updates updt_pk; Type: DEFAULT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.updates ALTER COLUMN updt_pk SET DEFAULT nextval('public.updates_updt_pk_seq'::regclass);


--
-- Name: certificates cert_order_uniq; Type: CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.certificates
    ADD CONSTRAINT cert_order_uniq UNIQUE (cert_order) DEFERRABLE;


--
-- Name: certificates cert_pk; Type: CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.certificates
    ADD CONSTRAINT cert_pk PRIMARY KEY (cert_pk);


--
-- Name: client client_pk; Type: CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.client
    ADD CONSTRAINT client_pk PRIMARY KEY (clnt_name);


--
-- Name: configs conf_pk; Type: CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.configs
    ADD CONSTRAINT conf_pk PRIMARY KEY (conf_pk);


--
-- Name: employees_voidings emce_pk; Type: CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.employees_voidings
    ADD CONSTRAINT emce_pk PRIMARY KEY (emvo_empl_fk, emvo_cert_fk);


--
-- Name: employees empl_pk; Type: CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.employees
    ADD CONSTRAINT empl_pk PRIMARY KEY (empl_pk);


--
-- Name: trainingtypes_certificates fk_combination_uniq; Type: CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.trainingtypes_certificates
    ADD CONSTRAINT fk_combination_uniq UNIQUE (ttce_trty_fk, ttce_cert_fk);


--
-- Name: trainingtypes order_uniq; Type: CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.trainingtypes
    ADD CONSTRAINT order_uniq UNIQUE (trty_order) DEFERRABLE;


--
-- Name: sites_tags sita_pk; Type: CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.sites_tags
    ADD CONSTRAINT sita_pk PRIMARY KEY (sita_site_fk, sita_tags_fk);


--
-- Name: sites site_pk; Type: CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.sites
    ADD CONSTRAINT site_pk PRIMARY KEY (site_pk);


--
-- Name: tags tags_order_uniq; Type: CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.tags
    ADD CONSTRAINT tags_order_uniq UNIQUE (tags_order) DEFERRABLE;


--
-- Name: tags tags_pk; Type: CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.tags
    ADD CONSTRAINT tags_pk PRIMARY KEY (tags_pk);


--
-- Name: trainerprofiles trainerprofiles_pk; Type: CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.trainerprofiles
    ADD CONSTRAINT trainerprofiles_pk PRIMARY KEY (trpr_pk);


--
-- Name: trainerprofiles_trainingtypes trainerprofiles_trainingtypes_pk; Type: CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.trainerprofiles_trainingtypes
    ADD CONSTRAINT trainerprofiles_trainingtypes_pk PRIMARY KEY (tptt_trpr_fk, tptt_trty_fk);


--
-- Name: trainings_employees trem_pk; Type: CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.trainings_employees
    ADD CONSTRAINT trem_pk PRIMARY KEY (trem_pk);


--
-- Name: trainings_trainers trng_empl_uniq; Type: CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.trainings_trainers
    ADD CONSTRAINT trng_empl_uniq UNIQUE (trtr_trng_fk, trtr_empl_fk);


--
-- Name: trainings trng_pk; Type: CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.trainings
    ADD CONSTRAINT trng_pk PRIMARY KEY (trng_pk);


--
-- Name: trainings_employees trng_trem_uniq; Type: CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.trainings_employees
    ADD CONSTRAINT trng_trem_uniq UNIQUE (trem_trng_fk, trem_empl_fk);


--
-- Name: trainingtypes trty_pk; Type: CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.trainingtypes
    ADD CONSTRAINT trty_pk PRIMARY KEY (trty_pk);


--
-- Name: sites_tags uniq (sita_site_fk, sita_tags_fk); Type: CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.sites_tags
    ADD CONSTRAINT "uniq (sita_site_fk, sita_tags_fk)" UNIQUE (sita_site_fk, sita_tags_fk);


--
-- Name: configs uniq on conf_type & conf_name; Type: CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.configs
    ADD CONSTRAINT "uniq on conf_type & conf_name" UNIQUE (conf_type, conf_name);


--
-- Name: employees uniq_empl_external_id; Type: CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.employees
    ADD CONSTRAINT uniq_empl_external_id UNIQUE (empl_external_id);


--
-- Name: sites uniq_site_external_id; Type: CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.sites
    ADD CONSTRAINT uniq_site_external_id UNIQUE (site_external_id);


--
-- Name: sites_employees updt_empl_uniq; Type: CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.sites_employees
    ADD CONSTRAINT updt_empl_uniq UNIQUE (siem_empl_fk, siem_updt_fk);


--
-- Name: updates updt_pk; Type: CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.updates
    ADD CONSTRAINT updt_pk PRIMARY KEY (updt_pk);


--
-- Name: users user_empl_fk_uniq; Type: CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT user_empl_fk_uniq UNIQUE (user_empl_fk);


--
-- Name: users user_pk; Type: CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT user_pk PRIMARY KEY (user_id);


--
-- Name: users user_site_fk_uniq; Type: CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT user_site_fk_uniq UNIQUE (user_site_fk);


--
-- Name: users_roles users_roles_pk; Type: CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.users_roles
    ADD CONSTRAINT users_roles_pk PRIMARY KEY (user_id, usro_type);


--
-- Name: empl_pk_idx; Type: INDEX; Schema: public; Owner: orcadb_root
--

CREATE INDEX empl_pk_idx ON public.employees USING btree (empl_pk);


--
-- Name: fki_siem_empl_fk; Type: INDEX; Schema: public; Owner: orcadb_root
--

CREATE INDEX fki_siem_empl_fk ON public.sites_employees USING btree (siem_empl_fk);


--
-- Name: fki_siem_site_fk; Type: INDEX; Schema: public; Owner: orcadb_root
--

CREATE INDEX fki_siem_site_fk ON public.sites_employees USING btree (siem_site_fk);


--
-- Name: fki_siem_updt_fk; Type: INDEX; Schema: public; Owner: orcadb_root
--

CREATE INDEX fki_siem_updt_fk ON public.sites_employees USING btree (siem_updt_fk);


--
-- Name: fki_trem_trng_fk; Type: INDEX; Schema: public; Owner: orcadb_root
--

CREATE INDEX fki_trem_trng_fk ON public.trainings_employees USING btree (trem_trng_fk);


--
-- Name: fki_trng_trty_fk; Type: INDEX; Schema: public; Owner: orcadb_root
--

CREATE INDEX fki_trng_trty_fk ON public.trainings USING btree (trng_trty_fk);


--
-- Name: site_pk_idx; Type: INDEX; Schema: public; Owner: orcadb_root
--

CREATE INDEX site_pk_idx ON public.sites USING btree (site_pk);


--
-- Name: trng_date_idx; Type: INDEX; Schema: public; Owner: orcadb_root
--

CREATE INDEX trng_date_idx ON public.trainings USING btree (trng_date);


--
-- Name: updt_date_idx; Type: INDEX; Schema: public; Owner: orcadb_root
--

CREATE INDEX updt_date_idx ON public.updates USING btree (updt_date);


--
-- Name: employees_voidings emvo_cert_fk; Type: FK CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.employees_voidings
    ADD CONSTRAINT emvo_cert_fk FOREIGN KEY (emvo_cert_fk) REFERENCES public.certificates(cert_pk) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Name: employees_voidings emvo_empl_fk; Type: FK CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.employees_voidings
    ADD CONSTRAINT emvo_empl_fk FOREIGN KEY (emvo_empl_fk) REFERENCES public.employees(empl_pk) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Name: trainingtypes_certificates fk_certificates_cert_pk; Type: FK CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.trainingtypes_certificates
    ADD CONSTRAINT fk_certificates_cert_pk FOREIGN KEY (ttce_cert_fk) REFERENCES public.certificates(cert_pk) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Name: trainingtypes_certificates fk_trainingtypes_trty_pk; Type: FK CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.trainingtypes_certificates
    ADD CONSTRAINT fk_trainingtypes_trty_pk FOREIGN KEY (ttce_trty_fk) REFERENCES public.trainingtypes(trty_pk) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Name: trainings fk_trainingtypes_trty_pk; Type: FK CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.trainings
    ADD CONSTRAINT fk_trainingtypes_trty_pk FOREIGN KEY (trng_trty_fk) REFERENCES public.trainingtypes(trty_pk) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Name: sites_employees fk_updates_updt_pk; Type: FK CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.sites_employees
    ADD CONSTRAINT fk_updates_updt_pk FOREIGN KEY (siem_updt_fk) REFERENCES public.updates(updt_pk) ON DELETE CASCADE;


--
-- Name: sites_employees siem_empl_fk; Type: FK CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.sites_employees
    ADD CONSTRAINT siem_empl_fk FOREIGN KEY (siem_empl_fk) REFERENCES public.employees(empl_pk) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Name: sites_employees siem_site_fk; Type: FK CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.sites_employees
    ADD CONSTRAINT siem_site_fk FOREIGN KEY (siem_site_fk) REFERENCES public.sites(site_pk) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Name: sites_tags sita_site_fk -> sites; Type: FK CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.sites_tags
    ADD CONSTRAINT "sita_site_fk -> sites" FOREIGN KEY (sita_site_fk) REFERENCES public.sites(site_pk) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Name: sites_tags sita_tags_fk -> tags; Type: FK CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.sites_tags
    ADD CONSTRAINT "sita_tags_fk -> tags" FOREIGN KEY (sita_tags_fk) REFERENCES public.tags(tags_pk) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Name: trainerprofiles_trainingtypes trainerprofiles_trainingtypes_trainerprofiles_fk; Type: FK CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.trainerprofiles_trainingtypes
    ADD CONSTRAINT trainerprofiles_trainingtypes_trainerprofiles_fk FOREIGN KEY (tptt_trpr_fk) REFERENCES public.trainerprofiles(trpr_pk) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Name: trainerprofiles_trainingtypes trainerprofiles_trainingtypes_trainingtypes_fk; Type: FK CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.trainerprofiles_trainingtypes
    ADD CONSTRAINT trainerprofiles_trainingtypes_trainingtypes_fk FOREIGN KEY (tptt_trty_fk) REFERENCES public.trainingtypes(trty_pk) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Name: trainings_employees trem_empl_fk; Type: FK CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.trainings_employees
    ADD CONSTRAINT trem_empl_fk FOREIGN KEY (trem_empl_fk) REFERENCES public.employees(empl_pk) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Name: trainings_employees trem_trng_fk; Type: FK CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.trainings_employees
    ADD CONSTRAINT trem_trng_fk FOREIGN KEY (trem_trng_fk) REFERENCES public.trainings(trng_pk) ON DELETE CASCADE;


--
-- Name: trainings_trainers trtr_empl_fk; Type: FK CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.trainings_trainers
    ADD CONSTRAINT trtr_empl_fk FOREIGN KEY (trtr_empl_fk) REFERENCES public.employees(empl_pk) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Name: trainings_trainers trtr_trng_fk; Type: FK CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.trainings_trainers
    ADD CONSTRAINT trtr_trng_fk FOREIGN KEY (trtr_trng_fk) REFERENCES public.trainings(trng_pk) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Name: users user_empl_fk; Type: FK CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT user_empl_fk FOREIGN KEY (user_empl_fk) REFERENCES public.employees(empl_pk) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Name: users_certificates users_certificates_certificates_fk; Type: FK CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.users_certificates
    ADD CONSTRAINT users_certificates_certificates_fk FOREIGN KEY (usce_cert_fk) REFERENCES public.certificates(cert_pk) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Name: users_certificates users_certificates_users_fk; Type: FK CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.users_certificates
    ADD CONSTRAINT users_certificates_users_fk FOREIGN KEY (usce_user_fk) REFERENCES public.users(user_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Name: users_roles users_roles_trainerprofiles_fk; Type: FK CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.users_roles
    ADD CONSTRAINT users_roles_trainerprofiles_fk FOREIGN KEY (usro_trpr_fk) REFERENCES public.trainerprofiles(trpr_pk) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Name: users_roles users_roles_users_fk; Type: FK CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.users_roles
    ADD CONSTRAINT users_roles_users_fk FOREIGN KEY (user_id) REFERENCES public.users(user_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Name: users users_sites_fk; Type: FK CONSTRAINT; Schema: public; Owner: orcadb_root
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_sites_fk FOREIGN KEY (user_site_fk) REFERENCES public.sites(site_pk) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Name: SCHEMA public; Type: ACL; Schema: -; Owner: orcadb_root
--

REVOKE USAGE ON SCHEMA public FROM PUBLIC;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- PostgreSQL database dump complete
--

