-- Create executions table if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = 'executions') THEN
        CREATE TABLE "executions"(
            "execution_id" UUID NOT NULL,
            "started_at" TIMESTAMP(0) WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
            "finished_at" TIMESTAMP(0) WITH TIME ZONE,
            "status" VARCHAR(255) NOT NULL DEFAULT 'RUNNING'
        );
        ALTER TABLE "executions" ADD PRIMARY KEY("execution_id");
        COMMENT ON TABLE "executions" IS 'Table storing execution information';
        COMMENT ON COLUMN "executions"."execution_id" IS 'Primary key for executions';
        COMMENT ON COLUMN "executions"."started_at" IS 'Timestamp when the execution started';
        COMMENT ON COLUMN "executions"."finished_at" IS 'Timestamp when the execution finished';
        COMMENT ON COLUMN "executions"."status" IS 'Status of the execution';
    END IF;
END $$;

-- Create classifiers table if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = 'classifiers') THEN
        CREATE TABLE "classifiers"(
            "classifier_id" BIGINT NOT NULL,
            "classifier_name" VARCHAR(255) NOT NULL,
            "gmail_query" VARCHAR(255) NOT NULL,
            "created_at" TIMESTAMP(0) WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
            "updated_at" TIMESTAMP(0) WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        ALTER TABLE "classifiers" ADD PRIMARY KEY("classifier_id");
        COMMENT ON TABLE "classifiers" IS 'Table storing classifier information';
        COMMENT ON COLUMN "classifiers"."classifier_id" IS 'Primary key for classifiers';
        COMMENT ON COLUMN "classifiers"."classifier_name" IS 'Name of the classifier';
        COMMENT ON COLUMN "classifiers"."gmail_query" IS 'Gmail query associated with the classifier';
        COMMENT ON COLUMN "classifiers"."created_at" IS 'Timestamp when the classifier was created';
        COMMENT ON COLUMN "classifiers"."updated_at" IS 'Timestamp when the classifier was last updated';
    END IF;
END $$;

-- Create classifier_executions table if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = 'classifier_executions') THEN
        CREATE TABLE "classifier_executions"(
            "run_id" UUID NOT NULL,
            "execution_id" UUID NOT NULL,
            "classifier_id" BIGINT NOT NULL,
            "started_at" TIMESTAMP(0) WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
            "finished_at" TIMESTAMP(0) WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
            "status" VARCHAR(255) NOT NULL
        );
        ALTER TABLE "classifier_executions" ADD PRIMARY KEY("run_id");
        COMMENT ON TABLE "classifier_executions" IS 'Table storing classifier execution information';
        COMMENT ON COLUMN "classifier_executions"."run_id" IS 'Primary key for classifier executions';
        COMMENT ON COLUMN "classifier_executions"."execution_id" IS 'Execution ID associated with the classifier execution';
        COMMENT ON COLUMN "classifier_executions"."classifier_id" IS 'Classifier ID associated with the execution';
        COMMENT ON COLUMN "classifier_executions"."started_at" IS 'Timestamp when the execution started';
        COMMENT ON COLUMN "classifier_executions"."finished_at" IS 'Timestamp when the execution finished';
        COMMENT ON COLUMN "classifier_executions"."status" IS 'Status of the execution';
    END IF;
END $$;

-- Create action_templates table if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = 'action_templates') THEN
        CREATE TABLE "action_templates"(
            "action_id" BIGINT NOT NULL,
            "action_name" VARCHAR(255) NOT NULL,
            "created_at" TIMESTAMP(0) WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
            "updated_at" TIMESTAMP(0) WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        ALTER TABLE "action_templates" ADD PRIMARY KEY("action_id");
        COMMENT ON TABLE "action_templates" IS 'Table storing action templates';
        COMMENT ON COLUMN "action_templates"."action_id" IS 'Primary key for action templates';
        COMMENT ON COLUMN "action_templates"."action_name" IS 'Name of the action template';
        COMMENT ON COLUMN "action_templates"."created_at" IS 'Timestamp when the action template was created';
        COMMENT ON COLUMN "action_templates"."updated_at" IS 'Timestamp when the action template was last updated';
    END IF;
END $$;

-- Create action_parameter_templates table if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = 'action_parameter_templates') THEN
        CREATE TABLE "action_parameter_templates"(
            "parameter_id" BIGINT NOT NULL,
            "action_id" BIGINT NOT NULL,
            "parameter_name" VARCHAR(255) NOT NULL,
            "parameter_type" VARCHAR(255) NOT NULL,
            "parameter_is_nullable" BOOLEAN NOT NULL,
            "parameter_default" VARCHAR(255) NOT NULL,
            "created_at" TIMESTAMP(0) WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
            "updated_at" TIMESTAMP(0) WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        ALTER TABLE "action_parameter_templates" ADD PRIMARY KEY("parameter_id");
        COMMENT ON TABLE "action_parameter_templates" IS 'Table storing action parameter templates';
        COMMENT ON COLUMN "action_parameter_templates"."parameter_id" IS 'Primary key for action parameter templates';
        COMMENT ON COLUMN "action_parameter_templates"."action_id" IS 'Action ID associated with the parameter';
        COMMENT ON COLUMN "action_parameter_templates"."parameter_name" IS 'Name of the parameter';
        COMMENT ON COLUMN "action_parameter_templates"."parameter_type" IS 'Type of the parameter';
        COMMENT ON COLUMN "action_parameter_templates"."parameter_is_nullable" IS 'Indicates if the parameter is nullable';
        COMMENT ON COLUMN "action_parameter_templates"."parameter_default" IS 'Default value of the parameter';
        COMMENT ON COLUMN "action_parameter_templates"."created_at" IS 'Timestamp when the parameter template was created';
        COMMENT ON COLUMN "action_parameter_templates"."updated_at" IS 'Timestamp when the parameter template was last updated';
    END IF;
END $$;

-- Create classifier_actions table if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = 'classifier_actions') THEN
        CREATE TABLE "classifier_actions"(
            "classifier_action_id" BIGINT NOT NULL,
            "classifier_id" BIGINT NOT NULL,
            "action_id" BIGINT NOT NULL
        );
        ALTER TABLE "classifier_actions" ADD PRIMARY KEY("classifier_action_id");
        COMMENT ON TABLE "classifier_actions" IS 'Table storing classifier actions';
        COMMENT ON COLUMN "classifier_actions"."classifier_action_id" IS 'Primary key for classifier actions';
        COMMENT ON COLUMN "classifier_actions"."classifier_id" IS 'Classifier ID associated with the action';
        COMMENT ON COLUMN "classifier_actions"."action_id" IS 'Action ID associated with the classifier action';
    END IF;
END $$;

-- Add foreign key constraints if they don't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'classifier_executions_classifier_id_foreign') THEN
        ALTER TABLE "classifier_executions" ADD CONSTRAINT "classifier_executions_classifier_id_foreign" FOREIGN KEY("classifier_id") REFERENCES "classifiers"("classifier_id");
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'classifier_executions_execution_id_foreign') THEN
        ALTER TABLE "classifier_executions" ADD CONSTRAINT "classifier_executions_execution_id_foreign" FOREIGN KEY("execution_id") REFERENCES "executions"("execution_id");
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'classifier_actions_action_id_foreign') THEN
        ALTER TABLE "classifier_actions" ADD CONSTRAINT "classifier_actions_action_id_foreign" FOREIGN KEY("action_id") REFERENCES "action_templates"("action_id");
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'classifier_actions_classifier_id_foreign') THEN
        ALTER TABLE "classifier_actions" ADD CONSTRAINT "classifier_actions_classifier_id_foreign" FOREIGN KEY("classifier_id") REFERENCES "classifiers"("classifier_id");
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'action_parameter_templates_action_id_foreign') THEN
        ALTER TABLE "action_parameter_templates" ADD CONSTRAINT "action_parameter_templates_action_id_foreign" FOREIGN KEY("action_id") REFERENCES "action_templates"("action_id");
    END IF;
END $$;