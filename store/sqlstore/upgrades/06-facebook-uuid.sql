-- v6: Add facebook_uuid column to device table
ALTER TABLE whatsmeow_device ADD facebook_uuid uniqueidentifier NULL;
