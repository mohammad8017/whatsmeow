-- v0 -> v9 (compatible with v8+): Latest schema
CREATE TABLE whatsmeow_device (
	jid VARCHAR(300) PRIMARY KEY ,
	lid VARCHAR(300),
	facebook_uuid UNIQUEIDENTIFIER,
	registration_id BIGINT NOT NULL CHECK ( registration_id >= 0 AND registration_id < 4294967296 ),
	noise_key    VARBINARY(max) NOT NULL CHECK ( LEN(noise_key) = 32 ),
	identity_key VARBINARY(max) NOT NULL CHECK ( LEN(identity_key) = 32 ),
	signed_pre_key     VARBINARY(max)   NOT NULL CHECK ( LEN(signed_pre_key) = 32 ),
	signed_pre_key_id  INTEGER NOT NULL CHECK ( signed_pre_key_id >= 0 AND signed_pre_key_id < 16777216 ),
	signed_pre_key_sig VARBINARY(max)   NOT NULL CHECK ( LEN(signed_pre_key_sig) = 64 ),
	adv_key         VARBINARY(max) NOT NULL,
	adv_details     VARBINARY(max) NOT NULL,
	adv_account_sig VARBINARY(max) NOT NULL CHECK ( LEN(adv_account_sig) = 64 ),
	adv_account_sig_key  VARBINARY(max) NOT NULL CHECK ( LEN(adv_account_sig_key) = 32 ),
	adv_device_sig  VARBINARY(max) NOT NULL CHECK ( LEN(adv_device_sig) = 64 ),
	platform      VARCHAR(300) NOT NULL DEFAULT '',
	business_name VARCHAR(300) NOT NULL DEFAULT '',
	push_name     VARCHAR(300) NOT NULL DEFAULT '',
	manager_id     VARCHAR(300) NOT NULL DEFAULT ''
);
CREATE TABLE whatsmeow_identity_keys (
	our_jid  VARCHAR(300),
	their_id VARCHAR(300),
	identity_info VARBINARY(max) NOT NULL CHECK ( LEN(identity_info) = 32 ),
	PRIMARY KEY (our_jid, their_id),
	FOREIGN KEY (our_jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE whatsmeow_pre_keys (
	jid      VARCHAR(300),
	key_id   INTEGER  CHECK ( key_id >= 0 AND key_id < 16777216 ),
	key_info       VARBINARY(max)  NOT NULL CHECK (LEN(key_info) = 32 ),
	uploaded BIT NOT NULL,
	PRIMARY KEY (jid, key_id),
	FOREIGN KEY (jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE whatsmeow_sessions (
	our_jid   VARCHAR(300),
	their_id  VARCHAR(300),
	session  VARBINARY(max),
	PRIMARY KEY (our_jid, their_id),
	FOREIGN KEY (our_jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE whatsmeow_sender_keys (
	our_jid     VARCHAR(300),
	chat_id     VARCHAR(300),
	sender_id  VARCHAR(300),
	sender_key  VARBINARY(max) NOT NULL,
	PRIMARY KEY (our_jid, chat_id, sender_id),
	FOREIGN KEY (our_jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE whatsmeow_app_state_sync_keys (
	jid         VARCHAR(300),
	key_id      VARBINARY(300),
	key_data    VARBINARY(max)  NOT NULL,
	timestamp_info   BIGINT NOT NULL,
	fingerprint VARBINARY(max)  NOT NULL,
	PRIMARY KEY (jid, key_id),
	FOREIGN KEY (jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE whatsmeow_app_state_version (
	jid     VARCHAR(300),
	name    VARCHAR(300),
	version_info BIGINT NOT NULL,
	hash    VARBINARY(max)  NOT NULL CHECK ( LEN(hash) = 128 ),
	PRIMARY KEY (jid, name),
	FOREIGN KEY (jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE whatsmeow_app_state_mutation_macs (
	jid        VARCHAR(300),
	name       VARCHAR(300),
	version_info   BIGINT,
	index_mac VARBINARY(200) CHECK ( LEN(index_mac) = 32 ),
	value_mac VARBINARY(max) NOT NULL CHECK ( LEN(value_mac) = 32 ),
	PRIMARY KEY (jid, name, version_info, index_mac),
	FOREIGN KEY (jid, name) REFERENCES whatsmeow_app_state_version(jid, name) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE whatsmeow_contacts (
	our_jid       VARCHAR(300),
	their_jid     VARCHAR(300),
	first_name    VARCHAR(300),
	full_name     VARCHAR(300),
	push_name     VARCHAR(300),
	business_name VARCHAR(300),
	PRIMARY KEY (our_jid, their_jid),
	FOREIGN KEY (our_jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE whatsmeow_chat_settings (
	our_jid       VARCHAR(300),
	chat_jid      VARCHAR(300),
	muted_until   BIGINT  NOT NULL DEFAULT 0,
	pinned        BIT NOT NULL DEFAULT 0,
	archived      BIT NOT NULL DEFAULT 1,
	PRIMARY KEY (our_jid, chat_jid),
	FOREIGN KEY (our_jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE whatsmeow_message_secrets (
	our_jid    VARCHAR(300),
	chat_jid   VARCHAR(200),
	sender_jid  VARCHAR(200),
	message_id VARCHAR(200),
	key_info  VARBINARY(max) NOT NULL,
	PRIMARY KEY (our_jid, chat_jid, sender_jid, message_id),
	FOREIGN KEY (our_jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE whatsmeow_privacy_tokens (
	our_jid   VARCHAR(300),
	their_jid VARCHAR(300),
	token     VARBINARY(max)  NOT NULL,
	timestamp_info BIGINT NOT NULL,
	PRIMARY KEY (our_jid, their_jid)
);

CREATE TABLE whatsmeow_lid_map (
    lid NVARCHAR(300) PRIMARY KEY,
    pn NVARCHAR(300) UNIQUE NOT NULL
);

CREATE TABLE whatsmeow_event_buffer (
	our_jid          VARCHAR(300)   NOT NULL,
	ciphertext_hash  VARBINARY(300)  NOT NULL CHECK ( LEN(ciphertext_hash) = 32 ),
	plaintext        VARBINARY(max),
	server_timestamp BIGINT NOT NULL,
	insert_timestamp BIGINT NOT NULL,
	PRIMARY KEY (our_jid, ciphertext_hash),
	FOREIGN KEY (our_jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
);
