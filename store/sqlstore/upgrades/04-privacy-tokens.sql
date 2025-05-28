-- v4: Add privacy tokens table
CREATE TABLE whatsmeow_privacy_tokens (
	our_jid   VARCHAR(300),
	their_jid VARCHAR(300),
	token     VARBINARY(max)  NOT NULL,
	timestamp_info BIGINT NOT NULL,
	PRIMARY KEY (our_jid, their_jid)
);
