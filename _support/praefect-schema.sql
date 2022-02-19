--
-- PostgreSQL database dump
--

-- Dumped from database version REPLACED
-- Dumped by pg_dump version REPLACED

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
-- Name: praefect_database_schema; Type: DATABASE; Schema: -; Owner: -
--

CREATE DATABASE praefect_database_schema WITH TEMPLATE = template0 ENCODING = 'UTF8' LC_COLLATE = 'en_US.utf8' LC_CTYPE = 'en_US.utf8';


\connect praefect_database_schema

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
-- Name: replication_job_state; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.replication_job_state AS ENUM (
    'ready',
    'in_progress',
    'completed',
    'cancelled',
    'failed',
    'dead'
);


--
-- Name: replication_job_type; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.replication_job_type AS ENUM (
    'update',
    'create',
    'delete',
    'delete_replica',
    'rename',
    'gc',
    'repack_full',
    'repack_incremental',
    'cleanup',
    'pack_refs',
    'write_commit_graph',
    'midx_repack',
    'optimize_repository',
    'prune_unreachable_objects'
);


--
-- Name: notify_on_change(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.notify_on_change() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
				DECLARE
					msg JSONB;
				BEGIN
				    	CASE TG_OP
					WHEN 'INSERT' THEN
						SELECT JSON_AGG(obj) INTO msg
						FROM (
							SELECT JSONB_BUILD_OBJECT('virtual_storage', virtual_storage, 'relative_paths', ARRAY_AGG(DISTINCT relative_path)) AS obj
							FROM NEW
							GROUP BY virtual_storage
						) t;
					WHEN 'UPDATE' THEN
						SELECT JSON_AGG(obj) INTO msg
						FROM (
							SELECT JSONB_BUILD_OBJECT('virtual_storage', virtual_storage, 'relative_paths', ARRAY_AGG(DISTINCT relative_path)) AS obj
							FROM NEW
							FULL JOIN OLD USING (virtual_storage, relative_path)
							GROUP BY virtual_storage
						) t;
					WHEN 'DELETE' THEN
						SELECT JSON_AGG(obj) INTO msg
						FROM (
							SELECT JSONB_BUILD_OBJECT('virtual_storage', virtual_storage, 'relative_paths', ARRAY_AGG(DISTINCT relative_path)) AS obj
							FROM OLD
							GROUP BY virtual_storage
						) t;
					END CASE;

				    	CASE WHEN JSONB_ARRAY_LENGTH(msg) > 0 THEN
						PERFORM PG_NOTIFY(TG_ARGV[TG_NARGS-1], msg::TEXT);
					ELSE END CASE;

					RETURN NULL;
				END;
				$$;


--
-- Name: remove_queue_lock_on_repository_removal(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.remove_queue_lock_on_repository_removal() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
			BEGIN
				DELETE FROM replication_queue_lock
				WHERE id LIKE (OLD.virtual_storage || '|%|' || OLD.relative_path);
				RETURN NULL;
		    	END;
			$$;


--
-- Name: replication_queue_flatten_job(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.replication_queue_flatten_job() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
			BEGIN
				NEW.change := (NEW.job->>'change')::REPLICATION_JOB_TYPE;
				NEW.repository_id := COALESCE(
					-- For the old jobs the repository_id field may be 'null' or not set at all.
					-- repository_id field could have 0 if event was created by reconciler before
					-- repositories table was populated with valid repository_id.
					CASE WHEN (NEW.job->>'repository_id')::BIGINT = 0 THEN NULL ELSE (NEW.job->>'repository_id')::BIGINT END,
					(SELECT repositories.repository_id FROM repositories WHERE repositories.virtual_storage = NEW.job->>'virtual_storage' AND repositories.relative_path = NEW.job->>'relative_path'),
					0
				);
				-- The reconciler doesn't populate replica_path field that is why we need make sure
				-- we have at least an empty value for the column, not to break scan operations.
				NEW.replica_path := COALESCE(NEW.job->>'replica_path', '');
				NEW.relative_path := NEW.job->>'relative_path';
				NEW.target_node_storage := NEW.job->>'target_node_storage';
				NEW.source_node_storage := COALESCE(NEW.job->>'source_node_storage', '');
				NEW.virtual_storage := NEW.job->>'virtual_storage';
				NEW.params := (NEW.job->>'params')::JSONB;
				RETURN NEW;
			END;
			$$;


SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: node_status; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.node_status (
    id bigint NOT NULL,
    praefect_name character varying(511) NOT NULL,
    shard_name character varying(255) NOT NULL,
    node_name character varying(255) NOT NULL,
    last_contact_attempt_at timestamp with time zone,
    last_seen_active_at timestamp with time zone
);


--
-- Name: healthy_storages; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.healthy_storages AS
 SELECT ns.shard_name AS virtual_storage,
    ns.node_name AS storage
   FROM public.node_status ns
  WHERE (ns.last_seen_active_at >= (now() - '00:00:10'::interval))
  GROUP BY ns.shard_name, ns.node_name
 HAVING ((count(ns.praefect_name))::numeric >= ( SELECT ceil(((count(DISTINCT node_status.praefect_name))::numeric / 2.0)) AS quorum_count
           FROM public.node_status
          WHERE (((node_status.shard_name)::text = (ns.shard_name)::text) AND (node_status.last_contact_attempt_at >= (now() - '00:01:00'::interval)))))
  ORDER BY ns.shard_name, ns.node_name;


--
-- Name: hello_world; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.hello_world (
    id integer
);


--
-- Name: node_status_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.node_status_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: node_status_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.node_status_id_seq OWNED BY public.node_status.id;


--
-- Name: replication_queue; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.replication_queue (
    id bigint NOT NULL,
    state public.replication_job_state DEFAULT 'ready'::public.replication_job_state NOT NULL,
    created_at timestamp without time zone DEFAULT timezone('UTC'::text, now()) NOT NULL,
    updated_at timestamp without time zone,
    attempt integer DEFAULT 3 NOT NULL,
    lock_id text,
    job jsonb,
    meta jsonb,
    change public.replication_job_type,
    repository_id bigint DEFAULT 0 NOT NULL,
    replica_path text DEFAULT ''::text NOT NULL,
    relative_path text,
    target_node_storage text,
    source_node_storage text DEFAULT ''::text NOT NULL,
    virtual_storage text,
    params jsonb
);


--
-- Name: replication_queue_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.replication_queue_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: replication_queue_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.replication_queue_id_seq OWNED BY public.replication_queue.id;


--
-- Name: replication_queue_job_lock; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.replication_queue_job_lock (
    job_id bigint NOT NULL,
    lock_id text NOT NULL,
    triggered_at timestamp without time zone DEFAULT timezone('UTC'::text, now()) NOT NULL
);


--
-- Name: replication_queue_lock; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.replication_queue_lock (
    id text NOT NULL,
    acquired boolean DEFAULT false NOT NULL
);


--
-- Name: repositories; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.repositories (
    virtual_storage text NOT NULL,
    relative_path text NOT NULL,
    generation bigint,
    "primary" text,
    repository_id bigint NOT NULL,
    replica_path text
);


--
-- Name: repositories_repository_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.repositories_repository_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: repositories_repository_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.repositories_repository_id_seq OWNED BY public.repositories.repository_id;


--
-- Name: repository_assignments; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.repository_assignments (
    virtual_storage text NOT NULL,
    relative_path text NOT NULL,
    storage text NOT NULL,
    repository_id bigint NOT NULL
);


--
-- Name: storage_repositories; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.storage_repositories (
    virtual_storage text NOT NULL,
    relative_path text NOT NULL,
    storage text NOT NULL,
    generation bigint NOT NULL,
    repository_id bigint NOT NULL
);


--
-- Name: repository_generations; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.repository_generations AS
 SELECT storage_repositories.virtual_storage,
    storage_repositories.relative_path,
    max(storage_repositories.generation) AS generation
   FROM public.storage_repositories
  GROUP BY storage_repositories.virtual_storage, storage_repositories.relative_path;


--
-- Name: schema_migrations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.schema_migrations (
    id text NOT NULL,
    applied_at timestamp with time zone
);


--
-- Name: shard_primaries; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.shard_primaries (
    id bigint NOT NULL,
    shard_name character varying(255) NOT NULL,
    node_name character varying(255) NOT NULL,
    elected_by_praefect character varying(255) NOT NULL,
    elected_at timestamp with time zone NOT NULL,
    read_only boolean DEFAULT false NOT NULL,
    demoted boolean DEFAULT false NOT NULL,
    previous_writable_primary text
);


--
-- Name: shard_primaries_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.shard_primaries_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: shard_primaries_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.shard_primaries_id_seq OWNED BY public.shard_primaries.id;


--
-- Name: storage_cleanups; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.storage_cleanups (
    virtual_storage text NOT NULL,
    storage text NOT NULL,
    last_run timestamp without time zone,
    triggered_at timestamp without time zone
);


--
-- Name: valid_primaries; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.valid_primaries AS
 SELECT candidates.repository_id,
    candidates.virtual_storage,
    candidates.relative_path,
    candidates.storage
   FROM ( SELECT repositories.repository_id,
            repositories.virtual_storage,
            repositories.relative_path,
            storage_repositories.storage,
            ((repository_assignments.storage IS NOT NULL) OR bool_and((repository_assignments.storage IS NULL)) OVER (PARTITION BY repositories.repository_id)) AS eligible
           FROM (((public.repositories
             JOIN ( SELECT storage_repositories_1.repository_id,
                    storage_repositories_1.storage,
                    storage_repositories_1.generation
                   FROM public.storage_repositories storage_repositories_1) storage_repositories USING (repository_id, generation))
             JOIN public.healthy_storages USING (virtual_storage, storage))
             LEFT JOIN public.repository_assignments USING (repository_id, storage))
          WHERE (NOT (EXISTS ( SELECT
                   FROM public.replication_queue queue
                  WHERE ((queue.state <> ALL (ARRAY['completed'::public.replication_job_state, 'dead'::public.replication_job_state, 'cancelled'::public.replication_job_state])) AND (queue.change = 'delete_replica'::public.replication_job_type) AND (queue.repository_id = storage_repositories.repository_id) AND (queue.target_node_storage = storage_repositories.storage)))))) candidates
  WHERE candidates.eligible;


--
-- Name: virtual_storages; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.virtual_storages (
    virtual_storage text NOT NULL,
    repositories_imported boolean DEFAULT false NOT NULL
);


--
-- Name: node_status id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.node_status ALTER COLUMN id SET DEFAULT nextval('public.node_status_id_seq'::regclass);


--
-- Name: replication_queue id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.replication_queue ALTER COLUMN id SET DEFAULT nextval('public.replication_queue_id_seq'::regclass);


--
-- Name: repositories repository_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.repositories ALTER COLUMN repository_id SET DEFAULT nextval('public.repositories_repository_id_seq'::regclass);


--
-- Name: shard_primaries id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.shard_primaries ALTER COLUMN id SET DEFAULT nextval('public.shard_primaries_id_seq'::regclass);


--
-- Name: node_status node_status_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.node_status
    ADD CONSTRAINT node_status_pkey PRIMARY KEY (id);


--
-- Name: replication_queue_job_lock replication_queue_job_lock_pk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.replication_queue_job_lock
    ADD CONSTRAINT replication_queue_job_lock_pk PRIMARY KEY (job_id, lock_id);


--
-- Name: replication_queue_lock replication_queue_lock_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.replication_queue_lock
    ADD CONSTRAINT replication_queue_lock_pkey PRIMARY KEY (id);


--
-- Name: replication_queue replication_queue_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.replication_queue
    ADD CONSTRAINT replication_queue_pkey PRIMARY KEY (id);


--
-- Name: repositories repositories_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.repositories
    ADD CONSTRAINT repositories_pkey PRIMARY KEY (repository_id);


--
-- Name: repository_assignments repository_assignments_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.repository_assignments
    ADD CONSTRAINT repository_assignments_pkey PRIMARY KEY (virtual_storage, relative_path, storage);


--
-- Name: schema_migrations schema_migrations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.schema_migrations
    ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (id);


--
-- Name: shard_primaries shard_primaries_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.shard_primaries
    ADD CONSTRAINT shard_primaries_pkey PRIMARY KEY (id);


--
-- Name: storage_cleanups storage_cleanups_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.storage_cleanups
    ADD CONSTRAINT storage_cleanups_pkey PRIMARY KEY (virtual_storage, storage);


--
-- Name: storage_repositories storage_repositories_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.storage_repositories
    ADD CONSTRAINT storage_repositories_pkey PRIMARY KEY (virtual_storage, relative_path, storage);


--
-- Name: virtual_storages virtual_storages_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.virtual_storages
    ADD CONSTRAINT virtual_storages_pkey PRIMARY KEY (virtual_storage);


--
-- Name: delete_replica_unique_index; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX delete_replica_unique_index ON public.replication_queue USING btree (virtual_storage, relative_path) WHERE ((state <> ALL (ARRAY['completed'::public.replication_job_state, 'cancelled'::public.replication_job_state, 'dead'::public.replication_job_state])) AND (change = 'delete_replica'::public.replication_job_type));


--
-- Name: replication_queue_target_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX replication_queue_target_index ON public.replication_queue USING btree (virtual_storage, relative_path, target_node_storage, change) WHERE (state <> ALL (ARRAY['completed'::public.replication_job_state, 'cancelled'::public.replication_job_state, 'dead'::public.replication_job_state]));


--
-- Name: repository_assignments_new_pkey; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX repository_assignments_new_pkey ON public.repository_assignments USING btree (repository_id, storage);


--
-- Name: repository_lookup_index; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX repository_lookup_index ON public.repositories USING btree (virtual_storage, relative_path);


--
-- Name: repository_replica_path_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX repository_replica_path_index ON public.repositories USING btree (replica_path, virtual_storage);


--
-- Name: shard_name_on_node_status_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX shard_name_on_node_status_idx ON public.node_status USING btree (shard_name, node_name);


--
-- Name: shard_name_on_shard_primaries_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX shard_name_on_shard_primaries_idx ON public.shard_primaries USING btree (shard_name);


--
-- Name: shard_node_names_on_node_status_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX shard_node_names_on_node_status_idx ON public.node_status USING btree (praefect_name, shard_name, node_name);


--
-- Name: storage_repositories_new_pkey; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX storage_repositories_new_pkey ON public.storage_repositories USING btree (repository_id, storage);


--
-- Name: virtual_target_on_replication_queue_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX virtual_target_on_replication_queue_idx ON public.replication_queue USING btree (virtual_storage, target_node_storage);


--
-- Name: repositories notify_on_delete; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER notify_on_delete AFTER DELETE ON public.repositories REFERENCING OLD TABLE AS old FOR EACH STATEMENT EXECUTE PROCEDURE public.notify_on_change('repositories_updates');


--
-- Name: storage_repositories notify_on_delete; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER notify_on_delete AFTER DELETE ON public.storage_repositories REFERENCING OLD TABLE AS old FOR EACH STATEMENT EXECUTE PROCEDURE public.notify_on_change('storage_repositories_updates');


--
-- Name: storage_repositories notify_on_insert; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER notify_on_insert AFTER INSERT ON public.storage_repositories REFERENCING NEW TABLE AS new FOR EACH STATEMENT EXECUTE PROCEDURE public.notify_on_change('storage_repositories_updates');


--
-- Name: storage_repositories notify_on_update; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER notify_on_update AFTER UPDATE ON public.storage_repositories REFERENCING OLD TABLE AS old NEW TABLE AS new FOR EACH STATEMENT EXECUTE PROCEDURE public.notify_on_change('storage_repositories_updates');


--
-- Name: repositories remove_queue_lock_on_repository_removal; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER remove_queue_lock_on_repository_removal AFTER DELETE ON public.repositories FOR EACH ROW EXECUTE PROCEDURE public.remove_queue_lock_on_repository_removal();


--
-- Name: replication_queue replication_queue_flatten_job; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER replication_queue_flatten_job BEFORE INSERT ON public.replication_queue FOR EACH ROW EXECUTE PROCEDURE public.replication_queue_flatten_job();


--
-- Name: replication_queue_job_lock replication_queue_job_lock_job_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.replication_queue_job_lock
    ADD CONSTRAINT replication_queue_job_lock_job_id_fkey FOREIGN KEY (job_id) REFERENCES public.replication_queue(id) ON DELETE CASCADE;


--
-- Name: replication_queue_job_lock replication_queue_job_lock_lock_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.replication_queue_job_lock
    ADD CONSTRAINT replication_queue_job_lock_lock_id_fkey FOREIGN KEY (lock_id) REFERENCES public.replication_queue_lock(id) ON DELETE CASCADE;


--
-- Name: replication_queue replication_queue_repository_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.replication_queue
    ADD CONSTRAINT replication_queue_repository_id_fkey FOREIGN KEY (repository_id) REFERENCES public.repositories(repository_id) ON DELETE CASCADE;


--
-- Name: repository_assignments repository_assignments_repository_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.repository_assignments
    ADD CONSTRAINT repository_assignments_repository_id_fkey FOREIGN KEY (repository_id) REFERENCES public.repositories(repository_id) ON DELETE CASCADE;


--
-- Name: repository_assignments repository_assignments_virtual_storage_relative_path_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.repository_assignments
    ADD CONSTRAINT repository_assignments_virtual_storage_relative_path_fkey FOREIGN KEY (virtual_storage, relative_path) REFERENCES public.repositories(virtual_storage, relative_path) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Name: storage_repositories storage_repositories_repository_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.storage_repositories
    ADD CONSTRAINT storage_repositories_repository_id_fkey FOREIGN KEY (repository_id) REFERENCES public.repositories(repository_id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

