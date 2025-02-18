BEGIN;

-- Insert statements for classifiers table
INSERT INTO "classifiers" ("classifier_name", "gmail_query")
VALUES 
('Spam Filter', 'label:spam'),
('Important Filter', 'label:important');

-- Insert statements for classifier_executions table
-- {# INSERT INTO "classifier_executions" ("classifier_execution_id", "execution_id", "classifier_id", "started_at", "finished_at", "status")
-- VALUES 
-- (uuid_generate_v4(), uuid_generate_v4(), 1, CURRENT_TIMESTAMP, NULL, 'RUNNING'),
-- (uuid_generate_v4(), uuid_generate_v4(), 2, CURRENT_TIMESTAMP, NULL, 'RUNNING'); #}

-- Insert statements for executions table
-- {# INSERT INTO "executions" ("execution_id", "started_at", "finished_at", "status")
-- VALUES 
-- (uuid_generate_v4(), CURRENT_TIMESTAMP, NULL, 'RUNNING'),
-- (uuid_generate_v4(), CURRENT_TIMESTAMP, NULL, 'RUNNING'); #}

-- Insert statements for action_templates table
{# INSERT INTO "action_templates" ("action_name")
VALUES 
('add_label'),
('mark_as_read'); #}

-- Insert statements for action_parameter_templates table
{# INSERT INTO "action_parameter_templates" ("action_id", "parameter_name", "parameter_type", "parameter_is_nullable", "parameter_default")
VALUES 
(1, 'label_name', 'VARCHAR', FALSE, 'Inbox'),
(2, 'read_status', 'BOOLEAN', FALSE, 'TRUE'); #}

-- Insert statements for classifier_actions table
{# INSERT INTO "classifier_actions" ("classifier_id", "action_id", "parameters")
VALUES 
(1, 1, '{"label_name": "Spam"}'),
(2, 2, '{"read_status": true}'); #}

-- Insert statements for classifier_messages_execution table
-- INSERT INTO "classifier_messages_execution" ("message_id", "classifier_execution_id", "started_at", "finished_at", "status")
-- VALUES 
-- ('msg-001', (SELECT "classifier_execution_id" FROM "classifier_executions" LIMIT 1), CURRENT_TIMESTAMP, NULL, 'RUNNING'),
-- ('msg-002', (SELECT "classifier_execution_id" FROM "classifier_executions" LIMIT 1 OFFSET 1), CURRENT_TIMESTAMP, NULL, 'RUNNING');

COMMIT;