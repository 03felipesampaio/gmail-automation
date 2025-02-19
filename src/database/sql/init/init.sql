DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'classifiers') THEN
        CREATE TABLE "classifiers"(
            "classifier_id" SERIAL NOT NULL,
            "classifier_name" VARCHAR(255) NOT NULL,
            "gmail_query" VARCHAR(255) NOT NULL,
            "created_at" TIMESTAMP(0) WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
            "updated_at" TIMESTAMP(0) WITH TIME ZONE
        );
        ALTER TABLE "classifiers" ADD PRIMARY KEY("classifier_id");
        ALTER TABLE "classifiers" ADD CONSTRAINT "unique_classifier_name" UNIQUE("classifier_name");
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'classifier_executions') THEN
        CREATE TABLE "classifier_executions"(
            "classifier_execution_id" UUID NOT NULL,
            "execution_id" UUID NOT NULL,
            "classifier_id" BIGINT NOT NULL,
            "started_at" TIMESTAMP(0) WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
            "finished_at" TIMESTAMP(0) WITH TIME ZONE,
            "status" VARCHAR(255) NOT NULL DEFAULT 'RUNNING'
        );
        ALTER TABLE "classifier_executions" ADD PRIMARY KEY("classifier_execution_id");
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'executions') THEN
        CREATE TABLE "executions"(
            "execution_id" UUID NOT NULL,
            "started_at" TIMESTAMP(0) WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
            "finished_at" TIMESTAMP(0) WITH TIME ZONE,
            "status" VARCHAR(255) NOT NULL DEFAULT 'RUNNING'
        );
        ALTER TABLE "executions" ADD PRIMARY KEY("execution_id");
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'action_templates') THEN
        CREATE TABLE "action_templates"(
            "action_name" VARCHAR(255) NOT NULL,
            "action_description" VARCHAR(255),
            "format" VARCHAR(255) NOT NULL,
            "created_at" TIMESTAMP(0) WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
            "updated_at" TIMESTAMP(0) WITH TIME ZONE
        );
        ALTER TABLE "action_templates" ADD PRIMARY KEY("action_name");
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'action_parameter_templates') THEN
        CREATE TABLE "action_parameter_templates"(
            "action_name" VARCHAR(255) NOT NULL,
            "parameter_name" VARCHAR(255) NOT NULL,
            "parameter_type" VARCHAR(255) NOT NULL,
            "parameter_is_nullable" BOOLEAN NOT NULL,
            "parameter_default" VARCHAR(255) NOT NULL,
            "created_at" TIMESTAMP(0) WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
            "updated_at" TIMESTAMP(0) WITH TIME ZONE
        );
        ALTER TABLE "action_parameter_templates" ADD PRIMARY KEY("action_name", "parameter_name");
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'classifier_actions') THEN
        CREATE TABLE "classifier_actions"(
            "classifier_action_id" SERIAL NOT NULL,
            "classifier_id" BIGINT NOT NULL,
            "action_name" VARCHAR(255) NOT NULL,
            "parameters" JSON NOT NULL
        );
        ALTER TABLE "classifier_actions" ADD PRIMARY KEY("classifier_action_id");
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'classifier_messages_execution') THEN
        CREATE TABLE "classifier_messages_execution"(
            "message_id" VARCHAR(255) NOT NULL,
            "classifier_execution_id" UUID NOT NULL,
            "started_at" TIMESTAMP(0) WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
            "finished_at" TIMESTAMP(0) WITH TIME ZONE,
            "status" VARCHAR(255) NOT NULL DEFAULT 'RUNNING'
        );
        ALTER TABLE "classifier_messages_execution" ADD PRIMARY KEY(
            "classifier_execution_id",
            "message_id"
        );
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.table_constraints WHERE constraint_name = 'classifier_messages_execution_classifier_execution_id_foreign') THEN
        ALTER TABLE "classifier_messages_execution" ADD CONSTRAINT "classifier_messages_execution_classifier_execution_id_foreign" FOREIGN KEY("classifier_execution_id") REFERENCES "classifier_executions"("classifier_execution_id");
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.table_constraints WHERE constraint_name = 'action_parameter_templates_action_id_foreign') THEN
        ALTER TABLE "action_parameter_templates" ADD CONSTRAINT "action_parameter_templates_action_id_foreign" FOREIGN KEY("action_name") REFERENCES "action_templates"("action_name");
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.table_constraints WHERE constraint_name = 'classifier_actions_action_id_foreign') THEN
        ALTER TABLE "classifier_actions" ADD CONSTRAINT "classifier_actions_action_id_foreign" FOREIGN KEY("action_name") REFERENCES "action_templates"("action_name");
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.table_constraints WHERE constraint_name = 'classifier_executions_classifier_id_foreign') THEN
        ALTER TABLE "classifier_executions" ADD CONSTRAINT "classifier_executions_classifier_id_foreign" FOREIGN KEY("classifier_id") REFERENCES "classifiers"("classifier_id");
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.table_constraints WHERE constraint_name = 'classifier_actions_classifier_id_foreign') THEN
        ALTER TABLE "classifier_actions" ADD CONSTRAINT "classifier_actions_classifier_id_foreign" FOREIGN KEY("classifier_id") REFERENCES "classifiers"("classifier_id");
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.table_constraints WHERE constraint_name = 'classifier_executions_execution_id_foreign') THEN
        ALTER TABLE "classifier_executions" ADD CONSTRAINT "classifier_executions_execution_id_foreign" FOREIGN KEY("execution_id") REFERENCES "executions"("execution_id");
    END IF;
END $$;
