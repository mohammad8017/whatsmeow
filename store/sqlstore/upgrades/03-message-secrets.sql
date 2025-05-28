-- v3: Add message secrets table
CREATE TABLE whatsmeow_message_secrets (
	our_jid    VARCHAR(300),
	chat_jid   VARCHAR(200),
	sender_jid  VARCHAR(200),
	message_id VARCHAR(200),
	key_info  VARBINARY(max) NOT NULL,
	PRIMARY KEY (our_jid, chat_jid, sender_jid, message_id),
	FOREIGN KEY (our_jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
);
