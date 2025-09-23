-- v11 (compatible with v8+): Store redacted phone number for LID members in groups
ALTER TABLE whatsmeow_contacts ADD redacted_phone VARCHAR(300);
