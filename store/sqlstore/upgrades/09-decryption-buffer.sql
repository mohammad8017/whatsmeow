-- v9 (compatible with v8+): Add decrypted event buffer
CREATE TABLE whatsmeow_event_buffer (
	our_jid          VARCHAR(300)   NOT NULL,
	ciphertext_hash  VARBINARY(300)  NOT NULL CHECK ( LEN(ciphertext_hash) = 32 ),
	plaintext        VARBINARY(max),
	server_timestamp BIGINT NOT NULL,
	insert_timestamp BIGINT NOT NULL,
	PRIMARY KEY (our_jid, ciphertext_hash),
	FOREIGN KEY (our_jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
);
