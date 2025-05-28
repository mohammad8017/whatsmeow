// Copyright (c) 2025 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package sqlstore contains an SQL-backed implementation of the interfaces in the store package.
package sqlstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	mssql "github.com/denisenkom/go-mssqldb"
	"go.mau.fi/util/dbutil"
	"go.mau.fi/util/exsync"
	waBinary "go.mau.fi/whatsmeow/binary"

	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/util/keys"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// ErrInvalidLength is returned by some database getters if the database returned a byte array with an unexpected length.
// This should be impossible, as the database schema contains CHECK()s for all the relevant columns.
var ErrInvalidLength = errors.New("database returned byte array with illegal length")

// PostgresArrayWrapper is a function to wrap array values before passing them to the sql package.
//
// When using github.com/lib/pq, you should set
//
//	whatsmeow.PostgresArrayWrapper = pq.Array
var PostgresArrayWrapper func(any) interface {
	driver.Valuer
	sql.Scanner
}

type SQLStore struct {
	*Container
	JID string

	preKeyLock sync.Mutex

	contactCache     map[types.JID]*types.ContactInfo
	contactCacheLock sync.Mutex

	migratedPNSessionsCache *exsync.Set[string]
}

type contactUpdate struct {
	sqlStore *SQLStore
	contacts []store.ContactEntry
}

type senderkeyUpdate struct {
	sqlStore *SQLStore
	group    string
	user     string
	session  []byte
}

type sessionUpdate struct {
	sqlStore *SQLStore
	address  string
	session  []byte
	isAdd    bool
}

type identityUpdate struct {
	sqlStore *SQLStore
	address  string
	key      [32]byte
	isAdd    bool
}

type removePreKeyUpdate struct {
	sqlStore *SQLStore
	id       uint32
}

type putMessageSecretUpdate struct {
	sqlStore *SQLStore
	chat     types.JID
	sender   types.JID
	id       types.MessageID
	secret   []byte
}

var sqlInstance *RetryDB
var contactsChannel = make(chan contactUpdate, 1000)
var senderkeysChannel = make(chan senderkeyUpdate, 1000)
var sessionChannel = make(chan sessionUpdate, 1000)
var identityChannel = make(chan identityUpdate, 1000)
var removePreKeyChannel = make(chan removePreKeyUpdate, 1000)
var putMessageSecretChannel = make(chan putMessageSecretUpdate, 1000)

// NewSQLStore creates a new SQLStore with the given database container and user JID.
// It contains implementations of all the different stores in the store package.
//
// In general, you should use Container.NewDevice or Container.GetDevice instead of this.
func NewSQLStore(c *Container, jid types.JID) *SQLStore {
	if sqlInstance == nil {
		sqlInstance = c.db
	}
	return &SQLStore{
		Container:    c,
		JID:          jid.String(),
		contactCache: make(map[types.JID]*types.ContactInfo),

		migratedPNSessionsCache: exsync.NewSet[string](),
	}
}

func ManageContacts(ctx context.Context, logger waLog.Logger) {
	defer func() {
		if value := recover(); value != nil {
			err, ok := value.(error)
			if ok {
				logger.Fatalf(err.Error())
			} else {
				logger.Fatalf("Manage Contacts Recovered:" + err.Error())
			}
		}
	}()
	for contactUpdate := range contactsChannel {
		err := bulkInsertContacts(ctx, contactUpdate)
		if err != nil {
			logger.Errorf("Could Not Insert Contacts: %s", err.Error())
		}
		contactUpdate.sqlStore.contactCacheLock.Lock()
		// Just clear the cache, fetching pushnames and business names would be too much effort
		// I'll do it myself then F U XD
		for _, updates := range contactUpdate.contacts {
			delete(contactUpdate.sqlStore.contactCache, updates.JID)
		}
		contactUpdate.sqlStore.contactCacheLock.Unlock()
	}
}

func bulkInsertContacts(ctx context.Context, update contactUpdate) error {
	return update.sqlStore.db.DoTxn(ctx, nil, func(ctx context.Context) error {
		dt := time.Now()
		_, err := update.sqlStore.db.Exec(ctx, fmt.Sprintf(`CREATE TABLE staging_contacts_%d (
			our_jid       VARCHAR(300),
			their_jid     VARCHAR(300),
			first_name    VARCHAR(300),
			full_name     VARCHAR(300)
		)`, dt.UnixMilli()))
		if err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
		bulkImportStr := mssql.CopyIn(fmt.Sprintf("staging_contacts_%d", dt.UnixMilli()), mssql.BulkOptions{}, "our_jid", "their_jid", "first_name", "full_name")
		stmt, err := update.sqlStore.db.PrepareContext(ctx, bulkImportStr)
		if err != nil {
			return fmt.Errorf("failed to prepare bulk: %w", err)
		}
		for _, insert := range update.contacts {
			_, err = stmt.Exec(update.sqlStore.JID, insert.JID.String(), insert.FirstName, insert.FullName)
			if err != nil {
				return fmt.Errorf("failed to prepare insert: %w", err)
			}
		}
		_, err = stmt.Exec()
		if err != nil {
			return fmt.Errorf("failed to execute bulk: %w", err)
		}
		_, err = update.sqlStore.db.Exec(ctx, fmt.Sprintf(`MERGE INTO whatsmeow_contacts AS target 
		USING staging_contacts_%d AS source 
		ON target.our_jid = source.our_jid AND target.their_jid = source.their_jid 
		WHEN MATCHED THEN
			UPDATE SET target.first_name = source.first_name, target.full_name = source.full_name
		WHEN NOT MATCHED THEN
			INSERT (our_jid, their_jid, first_name, full_name)
			VALUES (source.our_jid, source.their_jid, source.first_name, source.full_name);`, dt.UnixMilli()))
		if err != nil {
			return fmt.Errorf("failed to merge bulk: %w", err)
		}
		_, err = update.sqlStore.db.Exec(ctx, fmt.Sprintf("DROP TABLE staging_contacts_%d", dt.UnixMilli()))
		if err != nil {
			return fmt.Errorf("failed to drop table: %w", err)
		}
		return nil
	})
}

func ManageSenderKeys(ctx context.Context) {
	for update := range senderkeysChannel {
		err := manageSingleSenderKey(ctx, update)
		if err != nil {
			update.sqlStore.log.Errorf(err.Error())
		}
	}
}

func manageSingleSenderKey(ctx context.Context, senderkeyUpdate senderkeyUpdate) error {
	senderkeyUpdate.sqlStore.mutex.Lock()
	defer senderkeyUpdate.sqlStore.mutex.Unlock()
	if senderkeyUpdate.sqlStore.db.Dialect == dbutil.MSSQL {
		_, err := senderkeyUpdate.sqlStore.db.Exec(ctx, mssqlPutSenderKeyQuery, senderkeyUpdate.sqlStore.JID, senderkeyUpdate.group, senderkeyUpdate.user, senderkeyUpdate.session)
		return err
	}
	_, err := senderkeyUpdate.sqlStore.db.Exec(ctx, sqlitePutSenderKeyQuery, senderkeyUpdate.sqlStore.JID, senderkeyUpdate.group, senderkeyUpdate.user, senderkeyUpdate.session)
	return err
}

func ManageSessions(ctx context.Context, logger waLog.Logger) {
	for update := range sessionChannel {
		var err error
		if update.isAdd {
			err = putSingleSession(ctx, update)
		} else {
			err = deleteSingleSession(ctx, update)
		}
		if err != nil {
			logger.Errorf("Could Not Update Session %s: %s", update.address, err.Error())
		}
	}
}

func putSingleSession(ctx context.Context, update sessionUpdate) (err error) {
	update.sqlStore.mutex.Lock()
	defer update.sqlStore.mutex.Unlock()
	if update.sqlStore.db.Dialect == dbutil.MSSQL {
		_, err = update.sqlStore.db.Exec(ctx, mssqlPutSessionQuery, update.sqlStore.JID, update.address, update.session)
	} else {
		_, err = update.sqlStore.db.Exec(ctx, sqlitePutSessionQuery, update.sqlStore.JID, update.address, update.session)
	}
	return err
}

func deleteSingleSession(ctx context.Context, update sessionUpdate) (err error) {
	update.sqlStore.mutex.Lock()
	defer update.sqlStore.mutex.Unlock()
	_, err = update.sqlStore.db.Exec(ctx, deleteSessionQuery, update.sqlStore.JID, update.address)
	return err
}

func ManageIdentities(ctx context.Context, logger waLog.Logger) {
	for update := range identityChannel {
		var err error
		if update.isAdd {
			err = putSingleIdentity(ctx, update)
		} else {
			err = deleteSingleIdentity(ctx, update)
		}
		if err != nil {
			logger.Errorf("Could Not Update Identity %s: %s", update.address, err.Error())
		}
	}
}

func putSingleIdentity(ctx context.Context, update identityUpdate) (err error) {
	update.sqlStore.mutex.Lock()
	defer update.sqlStore.mutex.Unlock()
	if update.sqlStore.db.Dialect == dbutil.MSSQL {
		_, err := update.sqlStore.db.Exec(ctx, mssqlPutIdentityQuery, update.sqlStore.JID, update.address, update.key[:])
		return err
	}
	_, err = update.sqlStore.db.Exec(ctx, sqlitePutIdentityQuery, update.sqlStore.JID, update.address, update.key[:])
	return err
}

func deleteSingleIdentity(ctx context.Context, update identityUpdate) (err error) {
	update.sqlStore.mutex.Lock()
	defer update.sqlStore.mutex.Unlock()
	_, err = update.sqlStore.db.Exec(ctx, deleteAllIdentitiesQuery, update.sqlStore.JID, update.address)
	return err
}

func ManageRemovingPreKeys(ctx context.Context, logger waLog.Logger) {
	for update := range removePreKeyChannel {
		err := singleRemovePreKey(ctx, update)
		if err != nil {
			logger.Errorf("Could Not Remove PreKey %d: %s", update.id, err.Error())
		}
	}
}

func singleRemovePreKey(ctx context.Context, update removePreKeyUpdate) (err error) {
	update.sqlStore.mutex.Lock()
	defer update.sqlStore.mutex.Unlock()
	_, err = update.sqlStore.db.Exec(ctx, deletePreKeyQuery, update.sqlStore.JID, update.id)
	return err
}

func ManagePutMessageSecret(ctx context.Context, logger waLog.Logger) {
	for update := range putMessageSecretChannel {
		err := singlePutMessageSecret(ctx, update)
		if err != nil {
			logger.Errorf("Could Not Put Message Secret %s: %s", update.id, err.Error())
		}
	}
}

func singlePutMessageSecret(ctx context.Context, update putMessageSecretUpdate) (err error) {
	update.sqlStore.mutex.Lock()
	defer update.sqlStore.mutex.Unlock()
	if update.sqlStore.db.Dialect == dbutil.MSSQL {
		_, err = update.sqlStore.db.Exec(ctx, mssqlPutMsgSecret, update.sqlStore.JID, update.chat.ToNonAD(), update.sender.ToNonAD(), update.id, update.secret)
		return
	}
	_, err = update.sqlStore.db.Exec(ctx, sqlitePutMsgSecret, update.sqlStore.JID, update.chat.ToNonAD(), update.sender.ToNonAD(), update.id, update.secret)
	return
}

var _ store.AllSessionSpecificStores = (*SQLStore)(nil)

const (
	mssqlPutIdentityQuery = `
	MERGE INTO whatsmeow_identity_keys AS target
	USING (VALUES (@p1, @p2, @p3)) AS source (our_jid, their_id, identity_info)
	   ON (target.our_jid = source.our_jid AND target.their_id = source.their_id)
	WHEN MATCHED THEN
	   UPDATE SET target.identity_info = source.identity_info
	WHEN NOT MATCHED THEN
	   INSERT (our_jid, their_id, identity_info)
	   VALUES (source.our_jid, source.their_id, source.identity_info);
	`
	sqlitePutIdentityQuery = `
	INSERT INTO whatsmeow_identity_keys (our_jid, their_id, identity) VALUES (@p1, @p2, @p3)
		ON CONFLICT (our_jid, their_id) DO UPDATE SET identity=excluded.identity
	`
	deleteAllIdentitiesQuery = `DELETE FROM whatsmeow_identity_keys WHERE our_jid=@p1 AND their_id LIKE @p2`
	deleteIdentityQuery      = `DELETE FROM whatsmeow_identity_keys WHERE our_jid=@p1 AND their_id=@p2`
	sqliteGetIdentityQuery   = `SELECT identity FROM whatsmeow_identity_keys WHERE our_jid=@p1 AND their_id=@p2`
	mssqlGetIdentityQuery    = `SELECT identity_info FROM whatsmeow_identity_keys WITH (NOLOCK) WHERE our_jid=@p1 AND their_id=@p2`
)

func (s *SQLStore) PutIdentity(ctx context.Context, address string, key [32]byte) error {
	identityChannel <- identityUpdate{
		sqlStore: s,
		address:  address,
		key:      key,
		isAdd:    true,
	}
	return nil
}

func (s *SQLStore) DeleteAllIdentities(ctx context.Context, phone string) error {
	_, err := s.db.Exec(ctx, deleteAllIdentitiesQuery, s.JID, phone+":%")
	return err
}

func (s *SQLStore) DeleteIdentity(ctx context.Context, address string) error {
	identityChannel <- identityUpdate{
		sqlStore: s,
		address:  address,
		isAdd:    false,
	}
	return nil
}

func (s *SQLStore) IsTrustedIdentity(ctx context.Context, address string, key [32]byte) (bool, error) {
	var existingIdentity []byte
	var err error
	if s.db.Dialect == dbutil.MSSQL {
		err = s.db.QueryRow(ctx, mssqlGetIdentityQuery, s.JID, address).Scan(&existingIdentity)
	} else {
		err = s.db.QueryRow(ctx, sqliteGetIdentityQuery, s.JID, address).Scan(&existingIdentity)
	}
	if errors.Is(err, sql.ErrNoRows) {
		// Trust if not known, it'll be saved automatically later
		return true, nil
	} else if err != nil {
		return false, err
	} else if len(existingIdentity) != 32 {
		return false, ErrInvalidLength
	}
	return *(*[32]byte)(existingIdentity) == key, nil
}

const (
	getSessionQuery       = `SELECT session FROM whatsmeow_sessions WITH (NOLOCK) WHERE our_jid=@p1 AND their_id=@p2`
	sqliteHasSessionQuery = `SELECT true FROM whatsmeow_sessions WHERE our_jid=@p1 AND their_id=@p2`
	mssqlHasSessionQuery  = `SELECT 1 FROM whatsmeow_sessions WITH (NOLOCK) WHERE our_jid=@p1 AND their_id=@p2`
	mssqlHasDeviceManager = `SELECT 1 FROM whatsmeow_device WITH (NOLOCK) WHERE manager_id=@p1`
	sqlitePutSessionQuery = `
		INSERT INTO whatsmeow_sessions (our_jid, their_id, session) VALUES (@p1, @p2, @p3)
		ON CONFLICT (our_jid, their_id) DO UPDATE SET session=excluded.session
	`

	sqliteMigratePNToLIDSessionsQuery = `
		INSERT INTO whatsmeow_sessions (our_jid, their_id, session)
		SELECT our_jid, replace(their_id, $2, $3), session
		FROM whatsmeow_sessions
		WHERE our_jid=$1 AND their_id LIKE $2 || ':%'
		ON CONFLICT (our_jid, their_id) DO UPDATE SET session=excluded.session
	`
	mssqlMigratePNToLIDSessionsQuery = `
		MERGE INTO whatsmeow_sessions AS target
		USING (
    		SELECT our_jid, REPLACE(their_id, @p2, @p3) AS their_id, session
    		FROM whatsmeow_sessions
    		WHERE our_jid = @p1 AND their_id LIKE @p2 + ':%'
		) AS source
		ON target.our_jid = source.our_jid AND target.their_id = source.their_id
		WHEN MATCHED THEN
    		UPDATE SET session = source.session
		WHEN NOT MATCHED THEN
    		INSERT (our_jid, their_id, session)
    		VALUES (source.our_jid, source.their_id, source.session);`
	sqliteDeleteAllSenderKeysQuery        = `DELETE FROM whatsmeow_sender_keys WHERE our_jid=$1 AND sender_id LIKE $2`
	mssqlDeleteAllSenderKeysQuery         = `DELETE FROM whatsmeow_sender_keys WHERE our_jid=@p1 AND sender_id LIKE @p2`
	sqliteDeleteAllIdentityKeysQuery      = `DELETE FROM whatsmeow_identity_keys WHERE our_jid=$1 AND their_id LIKE $2`
	mssqlDeleteAllIdentityKeysQuery       = `DELETE FROM whatsmeow_identity_keys WHERE our_jid=@p1 AND their_id LIKE @p2`
	sqliteMigratePNToLIDIdentityKeysQuery = `
		INSERT INTO whatsmeow_identity_keys (our_jid, their_id, identity)
		SELECT our_jid, replace(their_id, $2, $3), identity
		FROM whatsmeow_identity_keys
		WHERE our_jid=$1 AND their_id LIKE $2 || ':%'
		ON CONFLICT (our_jid, their_id) DO UPDATE SET identity=excluded.identity
	`
	mssqlMigratePNToLIDIdentityKeysQuery = `
		MERGE INTO whatsmeow_identity_keys AS target
		USING (
    		SELECT our_jid, REPLACE(their_id, @p2, @p3) AS their_id, identity_info
    		FROM whatsmeow_identity_keys
    		WHERE our_jid = @p1 AND their_id LIKE @p2 + ':%'
		) AS source
		ON target.our_jid = source.our_jid AND target.their_id = source.their_id
		WHEN MATCHED THEN
    		UPDATE SET identity_info = source.identity_info
		WHEN NOT MATCHED THEN
    		INSERT (our_jid, their_id, identity_info)
    		VALUES (source.our_jid, source.their_id, source.identity_info);`
	sqliteMigratePNToLIDSenderKeysQuery = `
		INSERT INTO whatsmeow_sender_keys (our_jid, chat_id, sender_id, sender_key)
		SELECT our_jid, chat_id, replace(sender_id, $2, $3), sender_key
		FROM whatsmeow_sender_keys
		WHERE our_jid=$1 AND sender_id LIKE $2 || ':%'
		ON CONFLICT (our_jid, chat_id, sender_id) DO UPDATE SET sender_key=excluded.sender_key
	`
	mssqlMigratePNToLIDSenderKeysQuery = `
		MERGE INTO whatsmeow_sender_keys AS target
		USING (
    		SELECT our_jid, chat_id, REPLACE(sender_id, @p2, @p3) AS sender_id, sender_key
    		FROM whatsmeow_sender_keys
    		WHERE our_jid = @p1 AND sender_id LIKE @p2 + ':%'
		) AS source
		ON target.our_jid = source.our_jid AND target.chat_id = source.chat_id AND target.sender_id = source.sender_id
		WHEN MATCHED THEN
    		UPDATE SET sender_key = source.sender_key
		WHEN NOT MATCHED THEN
    		INSERT (our_jid, chat_id, sender_id, sender_key)
    		VALUES (source.our_jid, source.chat_id, source.sender_id, source.sender_key);`
	mssqlPutSessionQuery = `
	MERGE INTO whatsmeow_sessions AS target
	USING (VALUES (@p1, @p2, @p3)) AS source (our_jid, their_id, session)
	ON (target.our_jid = source.our_jid AND target.their_id = source.their_id)
	WHEN MATCHED THEN
    	UPDATE SET target.session = source.session
	WHEN NOT MATCHED THEN
    	INSERT (our_jid, their_id, session)
    	VALUES (source.our_jid, source.their_id, source.session);
	`
	deleteAllSessionsQuery = `DELETE FROM whatsmeow_sessions WHERE our_jid=@p1 AND their_id LIKE @p2`
	deleteSessionQuery     = `DELETE FROM whatsmeow_sessions WHERE our_jid=@p1 AND their_id=@p2`
)

func (s *SQLStore) GetSession(ctx context.Context, address string) (session []byte, err error) {
	err = s.db.QueryRow(ctx, getSessionQuery, s.JID, address).Scan(&session)
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
	}
	return
}

func (s *SQLStore) HasSession(ctx context.Context, address string) (has bool, err error) {
	if s.db.Dialect == dbutil.MSSQL {
		err = s.db.QueryRow(ctx, mssqlHasSessionQuery, s.JID, address).Scan(&has)
	} else {
		err = s.db.QueryRow(ctx, sqliteHasSessionQuery, s.JID, address).Scan(&has)
	}
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
	}
	return
}

func (s *SQLStore) PutSession(ctx context.Context, address string, session []byte) error {
	sessionChannel <- sessionUpdate{
		sqlStore: s,
		address:  address,
		session:  session,
		isAdd:    true,
	}
	return nil
}

func (s *SQLStore) DeleteAllSessions(ctx context.Context, phone string) error {
	return s.deleteAllSessions(ctx, phone)
}

func (s *SQLStore) deleteAllSessions(ctx context.Context, phone string) error {
	_, err := s.db.Exec(ctx, deleteAllSessionsQuery, s.JID, phone+":%")
	return err
}

func (s *SQLStore) deleteAllSenderKeys(ctx context.Context, phone string) error {
	if s.db.Dialect == dbutil.MSSQL {
		_, err := s.db.Exec(ctx, mssqlDeleteAllSenderKeysQuery, s.JID, phone+":%")
		return err
	}
	_, err := s.db.Exec(ctx, sqliteDeleteAllSenderKeysQuery, s.JID, phone+":%")
	return err
}

func (s *SQLStore) deleteAllIdentityKeys(ctx context.Context, phone string) error {
	if s.db.Dialect == dbutil.MSSQL {
		_, err := s.db.Exec(ctx, mssqlDeleteAllIdentityKeysQuery, s.JID, phone+":%")
		return err
	}
	_, err := s.db.Exec(ctx, sqliteDeleteAllIdentityKeysQuery, s.JID, phone+":%")
	return err
}

func (s *SQLStore) DeleteSession(ctx context.Context, address string) error {
	sessionChannel <- sessionUpdate{
		sqlStore: s,
		address:  address,
		isAdd:    false,
	}
	return nil
}

func (s *SQLStore) MigratePNToLID(ctx context.Context, pn, lid types.JID) error {
	pnSignal := pn.SignalAddressUser()
	if !s.migratedPNSessionsCache.Add(pnSignal) {
		return nil
	}
	var sessionsUpdated, identityKeysUpdated, senderKeysUpdated int64
	lidSignal := lid.SignalAddressUser()
	err := s.db.DoTxn(ctx, nil, func(ctx context.Context) error {
		var res sql.Result
		var err error
		if s.db.Dialect == dbutil.MSSQL {
			res, err = s.db.Exec(ctx, mssqlMigratePNToLIDSessionsQuery, s.JID, pnSignal, lidSignal)
		} else {
			res, err = s.db.Exec(ctx, sqliteMigratePNToLIDSessionsQuery, s.JID, pnSignal, lidSignal)
		}
		if err != nil {
			return fmt.Errorf("failed to migrate sessions: %w", err)
		}
		sessionsUpdated, err = res.RowsAffected()
		if err != nil {
			return fmt.Errorf("failed to get rows affected for sessions: %w", err)
		}
		err = s.deleteAllSessions(ctx, pnSignal)
		if err != nil {
			return fmt.Errorf("failed to delete extra sessions: %w", err)
		}
		if s.db.Dialect == dbutil.MSSQL {
			res, err = s.db.Exec(ctx, mssqlMigratePNToLIDSenderKeysQuery, s.JID, pnSignal, lidSignal)
		} else {
			res, err = s.db.Exec(ctx, sqliteMigratePNToLIDSenderKeysQuery, s.JID, pnSignal, lidSignal)
		}
		if err != nil {
			return fmt.Errorf("failed to migrate sender keys: %w", err)
		}
		senderKeysUpdated, err = res.RowsAffected()
		if err != nil {
			return fmt.Errorf("failed to get rows affected for sender keys: %w", err)
		}
		err = s.deleteAllSenderKeys(ctx, pnSignal)
		if err != nil {
			return fmt.Errorf("failed to delete extra sender keys: %w", err)
		}
		if s.db.Dialect == dbutil.MSSQL {
			res, err = s.db.Exec(ctx, mssqlMigratePNToLIDIdentityKeysQuery, s.JID, pnSignal, lidSignal)
			if err != nil {
				return fmt.Errorf("failed to migrate identity keys: %w", err)
			}
		} else {
			res, err = s.db.Exec(ctx, sqliteMigratePNToLIDIdentityKeysQuery, s.JID, pnSignal, lidSignal)
			if err != nil {
				return fmt.Errorf("failed to migrate identity keys: %w", err)
			}
		}

		identityKeysUpdated, err = res.RowsAffected()
		if err != nil {
			return fmt.Errorf("failed to get rows affected for identity keys: %w", err)
		}
		err = s.deleteAllIdentityKeys(ctx, pnSignal)
		if err != nil {
			return fmt.Errorf("failed to delete extra identity keys: %w", err)
		}

		return nil
	})
	if err != nil {
		return err
	}
	if sessionsUpdated > 0 || senderKeysUpdated > 0 || identityKeysUpdated > 0 {
		s.log.Infof("Migrated %d sessions, %d identity keys and %d sender keys from %s to %s", sessionsUpdated, identityKeysUpdated, senderKeysUpdated, pnSignal, lidSignal)
	} else {
		s.log.Debugf("No sessions or sender keys found to migrate from %s to %s", pnSignal, lidSignal)
	}
	return nil
}

const (
	getLastPreKeyIDQuery              = `SELECT MAX(key_id) FROM whatsmeow_pre_keys WITH (NOLOCK) WHERE jid=@p1`
	mssqlInsertPreKeyQuery            = `INSERT INTO whatsmeow_pre_keys (jid, key_id, key_info, uploaded) VALUES (@p1, @p2, @p3, @p4)`
	sqliteInsertPreKeyQuery           = `INSERT INTO whatsmeow_pre_keys (jid, key_id, key, uploaded) VALUES (@p1, @p2, @p3, @p4)`
	sqliteGetUnuploadedPreKeysQuery   = `SELECT key_id, key FROM whatsmeow_pre_keys WHERE jid=@p1 AND uploaded=false ORDER BY key_id LIMIT @p2`
	mssqlGetUnuploadedPreKeysQuery    = `SELECT TOP 1 key_id, key_info FROM whatsmeow_pre_keys WITH (NOLOCK) WHERE jid=@p2 AND uploaded=0 ORDER BY key_id`
	sqliteGetPreKeyQuery              = `SELECT key_id, key FROM whatsmeow_pre_keys WHERE jid=@p1 AND key_id=@p2`
	mssqlGetPreKeyQuery               = `SELECT key_id, key_info FROM whatsmeow_pre_keys WITH (NOLOCK) WHERE jid=@p1 AND key_id=@p2`
	deletePreKeyQuery                 = `DELETE FROM whatsmeow_pre_keys WHERE jid=@p1 AND key_id=@p2`
	mssqlMarkPreKeysAsUploadedQuery   = `UPDATE whatsmeow_pre_keys SET uploaded=1 WHERE jid=@p1 AND key_id<=@p2`
	sqliteMarkPreKeysAsUploadedQuery  = `UPDATE whatsmeow_pre_keys SET uploaded=true WHERE jid=@p1 AND key_id<=@p2`
	sqliteGetUploadedPreKeyCountQuery = `SELECT COUNT(*) FROM whatsmeow_pre_keys WHERE jid=@p1 AND uploaded=true`
	mssqlGetUploadedPreKeyCountQuery  = `SELECT COUNT(*) FROM whatsmeow_pre_keys WITH (NOLOCK) WHERE jid=@p1 AND uploaded=1`
)

func (s *SQLStore) genOnePreKey(ctx context.Context, id uint32, markUploaded bool) (*keys.PreKey, error) {
	key := keys.NewPreKey(id)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	var err error
	if s.db.Dialect == dbutil.MSSQL {
		_, err = s.db.Exec(ctx, mssqlInsertPreKeyQuery, s.JID, key.KeyID, key.Priv[:], markUploaded)
	} else {
		_, err = s.db.Exec(ctx, sqliteInsertPreKeyQuery, s.JID, key.KeyID, key.Priv[:], markUploaded)
	}

	return key, err
}

func (s *SQLStore) getNextPreKeyID(ctx context.Context) (uint32, error) {
	var lastKeyID sql.NullInt32
	err := s.db.QueryRow(ctx, getLastPreKeyIDQuery, s.JID).Scan(&lastKeyID)
	if err != nil {
		return 0, fmt.Errorf("failed to query next prekey ID: %w", err)
	}
	return uint32(lastKeyID.Int32) + 1, nil
}

func (s *SQLStore) GenOnePreKey(ctx context.Context) (*keys.PreKey, error) {
	s.preKeyLock.Lock()
	defer s.preKeyLock.Unlock()
	nextKeyID, err := s.getNextPreKeyID(ctx)
	if err != nil {
		return nil, err
	}
	return s.genOnePreKey(ctx, nextKeyID, true)
}

func (s *SQLStore) GetOrGenPreKeys(ctx context.Context, count uint32) ([]*keys.PreKey, error) {
	s.preKeyLock.Lock()
	defer s.preKeyLock.Unlock()
	var err error
	var res dbutil.Rows
	if s.db.Dialect == dbutil.MSSQL {
		res, err = s.db.Query(ctx, mssqlGetUnuploadedPreKeysQuery, count, s.JID)
	} else {
		res, err = s.db.Query(ctx, sqliteGetUnuploadedPreKeysQuery, s.JID, count)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query existing prekeys: %w", err)
	}
	newKeys := make([]*keys.PreKey, count)
	var existingCount uint32
	for res.Next() {
		var key *keys.PreKey
		key, err = scanPreKey(res)
		if err != nil {
			return nil, err
		} else if key != nil {
			newKeys[existingCount] = key
			existingCount++
		}
	}

	if existingCount < uint32(len(newKeys)) {
		var nextKeyID uint32
		nextKeyID, err = s.getNextPreKeyID(ctx)
		if err != nil {
			return nil, err
		}
		for i := existingCount; i < count; i++ {
			newKeys[i], err = s.genOnePreKey(ctx, nextKeyID, false)
			if err != nil {
				return nil, fmt.Errorf("failed to generate prekey: %w", err)
			}
			nextKeyID++
		}
	}

	return newKeys, nil
}

func scanPreKey(row dbutil.Scannable) (*keys.PreKey, error) {
	var priv []byte
	var id uint32
	err := row.Scan(&id, &priv)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, err
	} else if len(priv) != 32 {
		return nil, ErrInvalidLength
	}
	return &keys.PreKey{
		KeyPair: *keys.NewKeyPairFromPrivateKey(*(*[32]byte)(priv)),
		KeyID:   id,
	}, nil
}

func (s *SQLStore) GetPreKey(ctx context.Context, id uint32) (*keys.PreKey, error) {
	if s.db.Dialect == dbutil.MSSQL {
		return scanPreKey(s.db.QueryRow(ctx, mssqlGetPreKeyQuery, s.JID, id))
	}
	return scanPreKey(s.db.QueryRow(ctx, sqliteGetPreKeyQuery, s.JID, id))
}

func (s *SQLStore) RemovePreKey(ctx context.Context, id uint32) error {
	removePreKeyChannel <- removePreKeyUpdate{
		sqlStore: s,
		id:       id,
	}
	return nil
}

func (s *SQLStore) MarkPreKeysAsUploaded(ctx context.Context, upToID uint32) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.db.Dialect == dbutil.MSSQL {
		_, err := s.db.Exec(ctx, mssqlMarkPreKeysAsUploadedQuery, s.JID, upToID)
		return err
	}
	_, err := s.db.Exec(ctx, sqliteMarkPreKeysAsUploadedQuery, s.JID, upToID)
	return err
}

func (s *SQLStore) UploadedPreKeyCount(ctx context.Context) (count int, err error) {
	if s.db.Dialect == dbutil.MSSQL {
		err = s.db.QueryRow(ctx, mssqlGetUploadedPreKeyCountQuery, s.JID).Scan(&count)
		return
	}
	err = s.db.QueryRow(ctx, sqliteGetUploadedPreKeyCountQuery, s.JID).Scan(&count)
	return
}

const (
	getSenderKeyQuery       = `SELECT sender_key FROM whatsmeow_sender_keys WITH (NOLOCK) WHERE our_jid=@p1 AND chat_id=@p2 AND sender_id=@p3`
	sqlitePutSenderKeyQuery = `
		INSERT INTO whatsmeow_sender_keys (our_jid, chat_id, sender_id, sender_key) VALUES (@p1, @p2, @p3, @p4)
		ON CONFLICT (our_jid, chat_id, sender_id) DO UPDATE SET sender_key=excluded.sender_key
	`
	mssqlPutSenderKeyQuery = `
		MERGE INTO whatsmeow_sender_keys AS target
		USING (VALUES (@p1, @p2, @p3, @p4)) AS source (our_jid, chat_id, sender_id, sender_key)
		ON (target.our_jid = source.our_jid AND target.chat_id = source.chat_id AND target.sender_id = source.sender_id)
		WHEN MATCHED THEN
			UPDATE SET target.sender_key = source.sender_key
		WHEN NOT MATCHED THEN
			INSERT (our_jid, chat_id, sender_id, sender_key)
			VALUES (source.our_jid, source.chat_id, source.sender_id, source.sender_key);
	`
)

func (s *SQLStore) PutSenderKey(ctx context.Context, group, user string, session []byte) error {
	senderkeysChannel <- senderkeyUpdate{
		group:    group,
		user:     user,
		session:  session,
		sqlStore: s,
	}
	return nil
}

func (s *SQLStore) GetSenderKey(ctx context.Context, group, user string) (key []byte, err error) {
	err = s.db.QueryRow(ctx, getSenderKeyQuery, s.JID, group, user).Scan(&key)
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
	}
	return
}

const (
	sqlitePutAppStateSyncKeyQuery = `
		INSERT INTO whatsmeow_app_state_sync_keys (jid, key_id, key_data, timestamp, fingerprint) VALUES (@p1, @p2, @p3, @p4, @p5)
		ON CONFLICT (jid, key_id) DO UPDATE
			SET key_data=excluded.key_data, timestamp=excluded.timestamp, fingerprint=excluded.fingerprint
			WHERE excluded.timestamp > whatsmeow_app_state_sync_keys.timestamp
	`
	mssqlPutAppStateSyncKeyQuery = `
		MERGE INTO whatsmeow_app_state_sync_keys AS target
		USING (VALUES (@p1, @p2, @p3, @p4, @p5)) AS source (jid, key_id, key_data, timestamp_info, fingerprint)
		ON (target.jid = source.jid AND target.key_id = source.key_id)
		WHEN MATCHED AND source.timestamp_info > target.timestamp_info THEN
			UPDATE SET target.key_data = source.key_data, target.timestamp_info = source.timestamp_info, target.fingerprint = source.fingerprint
		WHEN NOT MATCHED THEN
			INSERT (jid, key_id, key_data, timestamp_info, fingerprint)
			VALUES (source.jid, source.key_id, source.key_data, source.timestamp_info, source.fingerprint);
	`
	sqliteGetAppStateSyncKeyQuery         = `SELECT key_data, timestamp, fingerprint FROM whatsmeow_app_state_sync_keys WHERE jid=@p1 AND key_id=@p2`
	mssqlGetAppStateSyncKeyQuery          = `SELECT key_data, timestamp_info, fingerprint FROM whatsmeow_app_state_sync_keys WITH (NOLOCK) WHERE jid=@p1 AND key_id=@p2`
	sqliteGetLatestAppStateSyncKeyIDQuery = `SELECT key_id FROM whatsmeow_app_state_sync_keys WHERE jid=@p1 ORDER BY timestamp DESC LIMIT 1`
	mssqlGetLatestAppStateSyncKeyIDQuery  = `SELECT TOP 1 key_id FROM whatsmeow_app_state_sync_keys WITH (NOLOCK) WHERE jid=@p1 ORDER BY timestamp_info DESC`
)

func (s *SQLStore) PutAppStateSyncKey(ctx context.Context, id []byte, key store.AppStateSyncKey) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.db.Dialect == dbutil.MSSQL {
		_, err := s.db.Exec(ctx, mssqlPutAppStateSyncKeyQuery, s.JID, id, key.Data, key.Timestamp, key.Fingerprint)
		return err
	}
	_, err := s.db.Exec(ctx, sqlitePutAppStateSyncKeyQuery, s.JID, id, key.Data, key.Timestamp, key.Fingerprint)
	return err
}

func (s *SQLStore) GetAppStateSyncKey(ctx context.Context, id []byte) (*store.AppStateSyncKey, error) {
	var key store.AppStateSyncKey
	var err error
	if s.db.Dialect == dbutil.MSSQL {
		err = s.db.QueryRow(ctx, mssqlGetAppStateSyncKeyQuery, s.JID, id).Scan(&key.Data, &key.Timestamp, &key.Fingerprint)
	} else {
		err = s.db.QueryRow(ctx, sqliteGetAppStateSyncKeyQuery, s.JID, id).Scan(&key.Data, &key.Timestamp, &key.Fingerprint)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return &key, err
}

func (s *SQLStore) GetLatestAppStateSyncKeyID(ctx context.Context) ([]byte, error) {
	var keyID []byte
	var err error
	if s.db.Dialect == dbutil.MSSQL {
		err = s.db.QueryRow(ctx, mssqlGetLatestAppStateSyncKeyIDQuery, s.JID).Scan(&keyID)
	} else {
		err = s.db.QueryRow(ctx, sqliteGetLatestAppStateSyncKeyIDQuery, s.JID).Scan(&keyID)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return keyID, err
}

const (
	sqlitePutAppStateVersionQuery = `
		INSERT INTO whatsmeow_app_state_version (jid, name, version, hash) VALUES (@p1, @p2, @p3, @p4)
		ON CONFLICT (jid, name) DO UPDATE SET version=excluded.version, hash=excluded.hash
	`
	mssqlPutAppStateVersionQuery = `
		MERGE INTO whatsmeow_app_state_version AS target
		USING (VALUES (@p1, @p2, @p3, @p4)) AS source (jid, name, version_info, hash)
		ON (target.jid = source.jid AND target.name = source.name)
		WHEN MATCHED THEN
			UPDATE SET target.version_info = source.version_info, target.hash = source.hash
		WHEN NOT MATCHED THEN
			INSERT (jid, name, version_info, hash)
			VALUES (source.jid, source.name, source.version_info, source.hash);
	`
	sqliteGetAppStateVersionQuery           = `SELECT version, hash FROM whatsmeow_app_state_version WHERE jid=@p1 AND name=@p2`
	mssqlGetAppStateVersionQuery            = `SELECT version_info, hash FROM whatsmeow_app_state_version WITH (NOLOCK) WHERE jid=@p1 AND name=@p2`
	deleteAppStateVersionQuery              = `DELETE FROM whatsmeow_app_state_version WHERE jid=@p1 AND name=@p2`
	sqlitePutAppStateMutationMACsQuery      = `INSERT INTO whatsmeow_app_state_mutation_macs (jid, name, version, index_mac, value_mac) VALUES `
	mssqlPutAppStateMutationMACsQuery       = `INSERT INTO whatsmeow_app_state_mutation_macs (jid, name, version_info, index_mac, value_mac) VALUES `
	deleteAppStateMutationMACsQueryPostgres = `DELETE FROM whatsmeow_app_state_mutation_macs WHERE jid=@p1 AND name=@p2 AND index_mac=ANY(@p3::bytea[])`
	deleteAppStateMutationMACsQueryGeneric  = `DELETE FROM whatsmeow_app_state_mutation_macs WHERE jid=@p1 AND name=@p2 AND index_mac IN `
	sqliteGetAppStateMutationMACQuery       = `SELECT value_mac FROM whatsmeow_app_state_mutation_macs WHERE jid=@p1 AND name=@p2 AND index_mac=@p3 ORDER BY version DESC LIMIT 1`
	mssqlGetAppStateMutationMACQuery        = `SELECT TOP 1 value_mac FROM whatsmeow_app_state_mutation_macs WITH (NOLOCK) WHERE jid=@p1 AND name=@p2 AND index_mac=@p3 ORDER BY version_info DESC`
)

func (s *SQLStore) PutAppStateVersion(ctx context.Context, name string, version uint64, hash [128]byte) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.db.Dialect == dbutil.MSSQL {
		_, err := s.db.Exec(ctx, mssqlPutAppStateVersionQuery, s.JID, name, version, hash[:])
		return err
	}
	_, err := s.db.Exec(ctx, sqlitePutAppStateVersionQuery, s.JID, name, version, hash[:])
	return err
}

func (s *SQLStore) GetAppStateVersion(ctx context.Context, name string) (version uint64, hash [128]byte, err error) {
	var uncheckedHash []byte
	if s.db.Dialect == dbutil.MSSQL {
		err = s.db.QueryRow(ctx, mssqlGetAppStateVersionQuery, s.JID, name).Scan(&version, &uncheckedHash)
	} else {
		err = s.db.QueryRow(ctx, sqliteGetAppStateVersionQuery, s.JID, name).Scan(&version, &uncheckedHash)
	}
	if errors.Is(err, sql.ErrNoRows) {
		// version will be 0 and hash will be an empty array, which is the correct initial state
		err = nil
	} else if err != nil {
		// There's an error, just return it
	} else if len(uncheckedHash) != 128 {
		// This shouldn't happen
		err = ErrInvalidLength
	} else {
		// No errors, convert hash slice to array
		hash = *(*[128]byte)(uncheckedHash)
	}
	return
}

func (s *SQLStore) DeleteAppStateVersion(ctx context.Context, name string) error {
	_, err := s.db.Exec(ctx, deleteAppStateVersionQuery, s.JID, name)
	return err
}

func (s *SQLStore) putAppStateMutationMACs(ctx context.Context, name string, version uint64, mutations []store.AppStateMutationMAC) error {
	values := make([]any, 3+len(mutations)*2)
	queryParts := make([]string, len(mutations))
	values[0] = s.JID
	values[1] = name
	values[2] = version
	placeholderSyntax := "(@p1, @p2, @p3, @p%d, @p%d)"
	if s.db.Dialect == dbutil.SQLite {
		placeholderSyntax = "(?1, ?2, ?3, ?%d, ?%d)"
	}
	for i, mutation := range mutations {
		baseIndex := 3 + i*2
		values[baseIndex] = mutation.IndexMAC
		values[baseIndex+1] = mutation.ValueMAC
		queryParts[i] = fmt.Sprintf(placeholderSyntax, baseIndex+1, baseIndex+2)
	}
	if s.db.Dialect == dbutil.MSSQL {
		_, err := s.db.Exec(ctx, mssqlPutAppStateMutationMACsQuery+strings.Join(queryParts, ","), values...)
		return err
	}
	_, err := s.db.Exec(ctx, sqlitePutAppStateMutationMACsQuery+strings.Join(queryParts, ","), values...)
	return err
}

const mutationBatchSize = 400

func (s *SQLStore) PutAppStateMutationMACs(ctx context.Context, name string, version uint64, mutations []store.AppStateMutationMAC) error {
	if len(mutations) == 0 {
		return nil
	}
	bulkimportStr := mssql.CopyIn("whatsmeow_app_state_mutation_macs", mssql.BulkOptions{}, "jid", "name", "version_info", "index_mac", "value_mac")
	stmt, err := s.db.PrepareContext(ctx, bulkimportStr)
	if err != nil {
		return fmt.Errorf("failed to prepare bulk: %w", err)
	}
	for _, mutation := range mutations {
		stmt.Exec(s.JID, name, version, mutation.IndexMAC[:], mutation.ValueMAC[:])
	}
	_, err = stmt.Exec()
	return err
}

func (s *SQLStore) DeleteAppStateMutationMACs(ctx context.Context, name string, indexMACs [][]byte) (err error) {
	if len(indexMACs) == 0 {
		return
	}
	if s.db.Dialect == dbutil.Postgres && PostgresArrayWrapper != nil {
		_, err = s.db.Exec(ctx, deleteAppStateMutationMACsQueryPostgres, s.JID, name, PostgresArrayWrapper(indexMACs))
	} else {
		args := make([]any, 2+len(indexMACs))
		args[0] = s.JID
		args[1] = name
		queryParts := make([]string, len(indexMACs))
		for i, item := range indexMACs {
			args[2+i] = item
			queryParts[i] = fmt.Sprintf("@p%d", i+3)
		}
		_, err = s.db.Exec(ctx, deleteAppStateMutationMACsQueryGeneric+"("+strings.Join(queryParts, ",")+")", args...)
	}
	return
}

func (s *SQLStore) GetAppStateMutationMAC(ctx context.Context, name string, indexMAC []byte) (valueMAC []byte, err error) {
	if s.db.Dialect == dbutil.MSSQL {
		err = s.db.QueryRow(ctx, mssqlGetAppStateMutationMACQuery, s.JID, name, indexMAC).Scan(&valueMAC)
	} else {
		err = s.db.QueryRow(ctx, sqliteGetAppStateMutationMACQuery, s.JID, name, indexMAC).Scan(&valueMAC)
	}
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
	}
	return
}

const (
	sqlitePutContactNameQuery = `
		INSERT INTO whatsmeow_contacts (our_jid, their_jid, first_name, full_name) VALUES (@p1, @p2, @p3, @p4)
		ON CONFLICT (our_jid, their_jid) DO UPDATE SET first_name=excluded.first_name, full_name=excluded.full_name
	`
	mssqlPutContactNameQuery = `
		MERGE INTO whatsmeow_contacts AS target
		USING (VALUES (@p1, @p2, @p3, @p4)) AS source (our_jid, their_jid, first_name, full_name)
		ON (target.our_jid = source.our_jid AND target.their_jid = source.their_jid)
		WHEN MATCHED THEN
			UPDATE SET target.first_name = source.first_name, target.full_name = source.full_name
		WHEN NOT MATCHED THEN
			INSERT (our_jid, their_jid, first_name, full_name)
			VALUES (source.our_jid, source.their_jid, source.first_name, source.full_name);
	`
	sqlitePutManyContactNamesQuery = `
		INSERT INTO whatsmeow_contacts (our_jid, their_jid, first_name, full_name)
		VALUES %s
		ON CONFLICT (our_jid, their_jid) DO UPDATE SET first_name=excluded.first_name, full_name=excluded.full_name
	`
	mssqlPutManyContactNamesQuery = `
		MERGE INTO whatsmeow_contacts AS target
		USING (VALUES %s) AS source (our_jid, their_jid, first_name, full_name)
		ON (target.our_jid = source.our_jid AND target.their_jid = source.their_jid)
		WHEN MATCHED THEN
			UPDATE SET target.first_name = source.first_name, target.full_name = source.full_name
		WHEN NOT MATCHED THEN
			INSERT (our_jid, their_jid, first_name, full_name)
			VALUES (source.our_jid, source.their_jid, source.first_name, source.full_name);
	`
	sqlitePutPushNameQuery = `
		INSERT INTO whatsmeow_contacts (our_jid, their_jid, push_name) VALUES (@p1, @p2, @p3)
		ON CONFLICT (our_jid, their_jid) DO UPDATE SET push_name=excluded.push_name
	`
	mssqlPutPushNameQuery = `
		MERGE INTO whatsmeow_contacts AS target
		USING (VALUES (@p1, @p2, @p3)) AS source (our_jid, their_jid, push_name)
		ON (target.our_jid = source.our_jid AND target.their_jid = source.their_jid)
		WHEN MATCHED THEN
			UPDATE SET target.push_name = source.push_name
		WHEN NOT MATCHED THEN
			INSERT (our_jid, their_jid, push_name)
			VALUES (source.our_jid, source.their_jid, source.push_name);
	`
	sqlitePutBusinessNameQuery = `
		INSERT INTO whatsmeow_contacts (our_jid, their_jid, business_name) VALUES (@p1, @p2, @p3)
		ON CONFLICT (our_jid, their_jid) DO UPDATE SET business_name=excluded.business_name
	`
	mssqlPutBusinessNameQuery = `
		MERGE INTO whatsmeow_contacts AS target
		USING (VALUES (@p1, @p2, @p3)) AS source (our_jid, their_jid, business_name)
		ON (target.our_jid = source.our_jid AND target.their_jid = source.their_jid)
		WHEN MATCHED THEN
			UPDATE SET target.business_name = source.business_name
		WHEN NOT MATCHED THEN
			INSERT (our_jid, their_jid, business_name)
			VALUES (source.our_jid, source.their_jid, source.business_name);
	`
	getContactQuery = `
		SELECT first_name, full_name, push_name, business_name FROM whatsmeow_contacts WITH (NOLOCK) WHERE our_jid=@p1 AND their_jid=@p2
	`
	getAllContactsQuery = `
		SELECT their_jid, first_name, full_name, push_name, business_name FROM whatsmeow_contacts WITH (NOLOCK) WHERE our_jid=@p1
	`
)

func (s *SQLStore) PutPushName(ctx context.Context, user types.JID, pushName string) (bool, string, error) {
	s.contactCacheLock.Lock()
	defer s.contactCacheLock.Unlock()

	cached, err := s.getContact(ctx, user)
	if err != nil {
		return false, "", err
	}
	if cached.PushName != pushName {
		if s.db.Dialect == dbutil.MSSQL {
			_, err = s.db.Exec(ctx, mssqlPutPushNameQuery, s.JID, user, pushName)
		} else {
			_, err = s.db.Exec(ctx, sqlitePutPushNameQuery, s.JID, user, pushName)
		}
		if err != nil {
			return false, "", err
		}
		previousName := cached.PushName
		cached.PushName = pushName
		cached.Found = true
		return true, previousName, nil
	}
	return false, "", nil
}

func (s *SQLStore) PutBusinessName(ctx context.Context, user types.JID, businessName string) (bool, string, error) {
	s.contactCacheLock.Lock()
	defer s.contactCacheLock.Unlock()

	cached, err := s.getContact(ctx, user)
	if err != nil {
		return false, "", err
	}
	if cached.BusinessName != businessName {
		if s.db.Dialect == dbutil.MSSQL {
			_, err = s.db.Exec(ctx, mssqlPutBusinessNameQuery, s.JID, user, businessName)
		} else {
			_, err = s.db.Exec(ctx, sqlitePutBusinessNameQuery, s.JID, user, businessName)
		}
		if err != nil {
			return false, "", err
		}
		previousName := cached.BusinessName
		cached.BusinessName = businessName
		cached.Found = true
		return true, previousName, nil
	}
	return false, "", nil
}

func (s *SQLStore) PutContactName(ctx context.Context, user types.JID, firstName, fullName string) error {
	s.contactCacheLock.Lock()
	defer s.contactCacheLock.Unlock()

	cached, err := s.getContact(ctx, user)
	if err != nil {
		return err
	}
	if cached.FirstName != firstName || cached.FullName != fullName {
		if s.db.Dialect == dbutil.MSSQL {
			_, err = s.db.Exec(ctx, mssqlPutContactNameQuery, s.JID, user, firstName, fullName)
		} else {
			_, err = s.db.Exec(ctx, sqlitePutContactNameQuery, s.JID, user, firstName, fullName)
		}
		if err != nil {
			return err
		}
		cached.FirstName = firstName
		cached.FullName = fullName
		cached.Found = true
	}
	return nil
}

const contactBatchSize = 300

func (s *SQLStore) putContactNamesBatch(ctx context.Context, contacts []store.ContactEntry) error {
	values := make([]any, 1, 1+len(contacts)*3)
	queryParts := make([]string, 0, len(contacts))
	values[0] = s.JID
	placeholderSyntax := "(@p1, @p%d, @p%d, @p%d)"
	if s.db.Dialect == dbutil.SQLite {
		placeholderSyntax = "(?1, ?%d, ?%d, ?%d)"
	}
	i := 0
	handledContacts := make(map[types.JID]struct{}, len(contacts))
	for _, contact := range contacts {
		if contact.JID.IsEmpty() {
			s.log.Warnf("Empty contact info in mass insert: %+v", contact)
			continue
		}
		// The whole query will break if there are duplicates, so make sure there aren't any duplicates
		_, alreadyHandled := handledContacts[contact.JID]
		if alreadyHandled {
			s.log.Warnf("Duplicate contact info for %s in mass insert", contact.JID)
			continue
		}
		handledContacts[contact.JID] = struct{}{}
		baseIndex := i*3 + 1
		values = append(values, contact.JID.String(), contact.FirstName, contact.FullName)
		queryParts = append(queryParts, fmt.Sprintf(placeholderSyntax, baseIndex+1, baseIndex+2, baseIndex+3))
		i++
	}
	if s.db.Dialect == dbutil.MSSQL {
		_, err := s.db.Exec(ctx, fmt.Sprintf(mssqlPutManyContactNamesQuery, strings.Join(queryParts, ",")), values...)
		return err
	}
	_, err := s.db.Exec(ctx, fmt.Sprintf(sqlitePutManyContactNamesQuery, strings.Join(queryParts, ",")), values...)
	return err
}

func (s *SQLStore) PutAllContactNames(ctx context.Context, contacts []store.ContactEntry) error {
	contactsChannel <- contactUpdate{
		sqlStore: s,
		contacts: contacts,
	}
	return nil
}

func (s *SQLStore) getContact(ctx context.Context, user types.JID) (*types.ContactInfo, error) {
	cached, ok := s.contactCache[user]
	if ok {
		return cached, nil
	}

	var first, full, push, business sql.NullString
	err := s.db.QueryRow(ctx, getContactQuery, s.JID, user).Scan(&first, &full, &push, &business)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	info := &types.ContactInfo{
		Found:        err == nil,
		FirstName:    first.String,
		FullName:     full.String,
		PushName:     push.String,
		BusinessName: business.String,
	}
	s.contactCache[user] = info
	return info, nil
}

func (s *SQLStore) GetContact(ctx context.Context, user types.JID) (types.ContactInfo, error) {
	s.contactCacheLock.Lock()
	info, err := s.getContact(ctx, user)
	s.contactCacheLock.Unlock()
	if err != nil {
		return types.ContactInfo{}, err
	}
	return *info, nil
}

func (s *SQLStore) GetAllContacts(ctx context.Context) (map[types.JID]types.ContactInfo, error) {
	s.contactCacheLock.Lock()
	defer s.contactCacheLock.Unlock()
	rows, err := s.db.Query(ctx, getAllContactsQuery, s.JID)
	if err != nil {
		return nil, err
	}
	output := make(map[types.JID]types.ContactInfo, len(s.contactCache))
	for rows.Next() {
		var jid types.JID
		var first, full, push, business sql.NullString
		err = rows.Scan(&jid, &first, &full, &push, &business)
		if err != nil {
			return nil, fmt.Errorf("error scanning row: %w", err)
		}
		info := types.ContactInfo{
			Found:        true,
			FirstName:    first.String,
			FullName:     full.String,
			PushName:     push.String,
			BusinessName: business.String,
		}
		output[jid] = info
		s.contactCache[jid] = &info
	}
	return output, nil
}

const (
	sqlitePutChatSettingQuery = `
		INSERT INTO whatsmeow_chat_settings (our_jid, chat_jid, %[1]s) VALUES (@p1, @p2, @p3)
		ON CONFLICT (our_jid, chat_jid) DO UPDATE SET %[1]s=excluded.%[1]s
	`
	mssqlPutChatSettingQuery = `
		MERGE INTO whatsmeow_chat_settings AS target
		USING (VALUES (@p1, @p2, @p3)) AS source (our_jid, chat_jid, %[1]s)
		ON (target.our_jid = source.our_jid AND target.chat_jid = source.chat_jid)
		WHEN MATCHED THEN
			UPDATE SET target.%[1]s = source.%[1]s
		WHEN NOT MATCHED THEN
			INSERT (our_jid, chat_jid, %[1]s)
			VALUES (source.our_jid, source.chat_jid, source.%[1]s);
	`
	getChatSettingsQuery = `
		SELECT muted_until, pinned, archived FROM whatsmeow_chat_settings WITH (NOLOCK) WHERE our_jid=@p1 AND chat_jid=@p2
	`
)

func (s *SQLStore) PutMutedUntil(ctx context.Context, chat types.JID, mutedUntil time.Time) error {
	var val int64
	if !mutedUntil.IsZero() {
		val = mutedUntil.Unix()
	}
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.db.Dialect == dbutil.MSSQL {
		_, err := s.db.Exec(ctx, fmt.Sprintf(mssqlPutChatSettingQuery, "muted_until"), s.JID, chat, val)
		return err
	}
	_, err := s.db.Exec(ctx, fmt.Sprintf(sqlitePutChatSettingQuery, "muted_until"), s.JID, chat, val)
	return err
}

func (s *SQLStore) PutPinned(ctx context.Context, chat types.JID, pinned bool) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.db.Dialect == dbutil.MSSQL {
		_, err := s.db.Exec(ctx, fmt.Sprintf(mssqlPutChatSettingQuery, "pinned"), s.JID, chat, pinned)
		return err
	}
	_, err := s.db.Exec(ctx, fmt.Sprintf(sqlitePutChatSettingQuery, "pinned"), s.JID, chat, pinned)
	return err
}

func (s *SQLStore) PutArchived(ctx context.Context, chat types.JID, archived bool) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.db.Dialect == dbutil.MSSQL {
		_, err := s.db.Exec(ctx, fmt.Sprintf(mssqlPutChatSettingQuery, "archived"), s.JID, chat, archived)
		return err
	}
	_, err := s.db.Exec(ctx, fmt.Sprintf(sqlitePutChatSettingQuery, "archived"), s.JID, chat, archived)
	return err
}

func (s *SQLStore) GetChatSettings(ctx context.Context, chat types.JID) (settings types.LocalChatSettings, err error) {
	var mutedUntil int64
	err = s.db.QueryRow(ctx, getChatSettingsQuery, s.JID, chat).Scan(&mutedUntil, &settings.Pinned, &settings.Archived)
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
	} else if err != nil {
		return
	} else {
		settings.Found = true
	}
	if mutedUntil != 0 {
		settings.MutedUntil = time.Unix(mutedUntil, 0)
	}
	return
}

const (
	sqlitePutMsgSecret = `
		INSERT INTO whatsmeow_message_secrets (our_jid, chat_jid, sender_jid, message_id, key)
		VALUES (@p1, @p2, @p3, @p4, @p5)
		ON CONFLICT (our_jid, chat_jid, sender_jid, message_id) DO NOTHING
	`
	mssqlPutMsgSecret = `
		INSERT INTO whatsmeow_message_secrets (our_jid, chat_jid, sender_jid, message_id, key_info)
		SELECT @p1, @p2, @p3, @p4, @p5
		WHERE NOT EXISTS (
			SELECT 1
			FROM whatsmeow_message_secrets
			WHERE our_jid = @p1
				AND chat_jid = @p2
				AND sender_jid = @p3
				AND message_id = @p4
		);
	`
	sqliteGetMsgSecret = `
		SELECT key FROM whatsmeow_message_secrets WHERE our_jid=@p1 AND chat_jid=@p2 AND sender_jid=@p3 AND message_id=@p4
	`
	mssqlGetMsgSecret = `
		SELECT key_info FROM whatsmeow_message_secrets WITH (NOLOCK) WHERE our_jid=@p1 AND chat_jid=@p2 AND sender_jid=@p3 AND message_id=@p4
	`
)

func (s *SQLStore) PutMessageSecrets(ctx context.Context, inserts []store.MessageSecretInsert) (err error) {
	if len(inserts) == 0 {
		return nil
	}
	return s.db.DoTxn(ctx, nil, func(ctx context.Context) error {
		dt := time.Now()
		_, err = s.db.Exec(ctx, fmt.Sprintf(`CREATE TABLE staging_messagesecrets_%d (
			our_jid    VARCHAR(300),
			chat_jid   VARCHAR(200),
			sender_jid  VARCHAR(200),
			message_id VARCHAR(200),
			key_info  VARBINARY(max) NOT NULL
		)
	`, dt.UnixMilli()))
		if err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
		bulkImportStr := mssql.CopyIn(fmt.Sprintf("staging_messagesecrets_%d", dt.UnixMilli()), mssql.BulkOptions{}, "our_jid", "chat_jid", "sender_jid", "message_id", "key_info")
		stmt, err := s.db.PrepareContext(ctx, bulkImportStr)
		if err != nil {
			return fmt.Errorf("failed to prepare bulk: %w", err)
		}
		for _, insert := range inserts {
			_, err = stmt.Exec(s.JID, insert.Chat.ToNonAD(), insert.Sender.ToNonAD(), insert.ID, insert.Secret)
			if err != nil {
				return fmt.Errorf("failed to prepare insert: %w", err)
			}
		}
		_, err = stmt.Exec()
		if err != nil {
			return fmt.Errorf("failed to execute bulk: %w", err)
		}
		_, err = s.db.Exec(ctx, fmt.Sprintf("MERGE INTO whatsmeow_message_secrets AS target USING staging_messagesecrets_%d AS source ON target.our_jid = source.our_jid AND target.chat_jid = source.chat_jid AND target.sender_jid = source.sender_jid AND target.message_id = source.message_id WHEN NOT MATCHED THEN INSERT (our_jid, chat_jid, sender_jid, message_id, key_info) VALUES (source.our_jid, source.chat_jid, source.sender_jid, source.message_id, source.key_info);", dt.UnixMilli()))
		if err != nil {
			return fmt.Errorf("failed to merge bulk: %w", err)
		}
		_, err = s.db.Exec(ctx, fmt.Sprintf("DROP TABLE staging_messagesecrets_%d", dt.UnixMilli()))
		if err != nil {
			return fmt.Errorf("failed to drop table: %w", err)
		}
		if err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}
		return nil
	})
}

func (s *SQLStore) PutMessageSecret(ctx context.Context, chat, sender types.JID, id types.MessageID, secret []byte) (err error) {
	putMessageSecretChannel <- putMessageSecretUpdate{
		sqlStore: s,
		chat:     chat,
		sender:   sender,
		id:       id,
		secret:   secret,
	}
	return nil
}

func (s *SQLStore) GetMessageSecret(ctx context.Context, chat, sender types.JID, id types.MessageID) (secret []byte, err error) {
	if s.db.Dialect == dbutil.MSSQL {
		err = s.db.QueryRow(ctx, mssqlGetMsgSecret, s.JID, chat.ToNonAD(), sender.ToNonAD(), id).Scan(&secret)
	} else {
		err = s.db.QueryRow(ctx, sqliteGetMsgSecret, s.JID, chat.ToNonAD(), sender.ToNonAD(), id).Scan(&secret)
	}
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
	}
	return
}

const (
	sqlitePutPrivacyTokens = `
		INSERT INTO whatsmeow_privacy_tokens (our_jid, their_jid, token, timestamp)
		VALUES (@p1, @p2, @p3, @p4)
		ON CONFLICT (our_jid, their_jid) DO UPDATE SET token=EXCLUDED.token, timestamp=EXCLUDED.timestamp
	`
	mssqlPutPrivacyTokens = `
		MERGE INTO whatsmeow_privacy_tokens AS target
		USING (VALUES (@p1, @p2, @p3, @p4)) AS source (our_jid, their_jid, token, timestamp_info)
		ON (target.our_jid = source.our_jid AND target.their_jid = source.their_jid)
		WHEN MATCHED THEN
			UPDATE SET target.token = source.token, target.timestamp_info = source.timestamp_info
		WHEN NOT MATCHED THEN
			INSERT (our_jid, their_jid, token, timestamp_info)
			VALUES (source.our_jid, source.their_jid, source.token, source.timestamp_info);
	`
	sqliteGetPrivacyToken = `SELECT token, timestamp FROM whatsmeow_privacy_tokens WHERE our_jid=@p1 AND their_jid=@p2`
	mssqlGetPrivacyToken  = `SELECT token, timestamp_info FROM whatsmeow_privacy_tokens WITH (NOLOCK) WHERE our_jid=@p1 AND their_jid=@p2`
)

func (s *SQLStore) PutPrivacyTokens(ctx context.Context, tokens ...store.PrivacyToken) error {
	return s.db.DoTxn(ctx, nil, func(ctx context.Context) error {
		dt := time.Now()
		_, err := s.db.Exec(ctx, fmt.Sprintf(`CREATE TABLE staging_privacytokens_%d (
			our_jid   VARCHAR(300),
			their_jid VARCHAR(300),
			token     VARBINARY(max)  NOT NULL,
			timestamp_info BIGINT NOT NULL,
		)`, dt.UnixMilli()))
		if err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
		bulkImportStr := mssql.CopyIn(fmt.Sprintf("staging_privacytokens_%d", dt.UnixMilli()), mssql.BulkOptions{}, "our_jid", "their_jid", "token", "timestamp_info")
		stmt, err := s.db.PrepareContext(ctx, bulkImportStr)
		if err != nil {
			return fmt.Errorf("failed to prepare bulk: %w", err)
		}
		for _, token := range tokens {
			_, err = stmt.Exec(s.JID, token.User.ToNonAD().String(), token.Token, token.Timestamp.Unix())
			if err != nil {
				return fmt.Errorf("failed to prepare insert: %w", err)
			}
		}
		_, err = stmt.Exec()
		if err != nil {
			return fmt.Errorf("failed to execute bulk: %w", err)
		}
		_, err = s.db.Exec(ctx, fmt.Sprintf(`MERGE INTO whatsmeow_privacy_tokens AS target 
		USING staging_privacytokens_%d AS source 
		ON target.our_jid = source.our_jid AND target.their_jid = source.their_jid
		WHEN MATCHED THEN
			UPDATE SET target.token = source.token, target.timestamp_info = source.timestamp_info
		WHEN NOT MATCHED THEN
			INSERT (our_jid, their_jid, token, timestamp_info)
			VALUES (source.our_jid, source.their_jid, source.token, source.timestamp_info);`, dt.UnixMilli()))
		if err != nil {
			return fmt.Errorf("failed to merge bulk: %w", err)
		}
		_, err = s.db.Exec(ctx, fmt.Sprintf("DROP TABLE staging_privacytokens_%d", dt.UnixMilli()))
		if err != nil {
			return fmt.Errorf("failed to drop table: %w", err)
		}
		if err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}
		return nil
	})
}

func (s *SQLStore) GetPrivacyToken(ctx context.Context, user types.JID) (*store.PrivacyToken, error) {
	var token store.PrivacyToken
	token.User = user.ToNonAD()
	var ts int64
	var err error
	if s.db.Dialect == dbutil.MSSQL {
		err = s.db.QueryRow(ctx, mssqlGetPrivacyToken, s.JID, token.User).Scan(&token.Token, &ts)
	} else {
		err = s.db.QueryRow(ctx, sqliteGetPrivacyToken, s.JID, token.User).Scan(&token.Token, &ts)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, err
	} else {
		token.Timestamp = time.Unix(ts, 0)
		return &token, nil
	}
}

const (
	getCacheSessionQuery      = `SELECT their_id, session FROM whatsmeow_sessions WITH (NOLOCK) WHERE our_jid=@p1 AND their_id IN `
	getCacheIdentityQuery     = `SELECT their_id, identity_info FROM whatsmeow_identity_keys WITH (NOLOCK) WHERE our_jid=@p1 AND their_id IN `
	removeSessionsQuery       = `DELETE FROM whatsmeow_sessions WHERE our_jid=@p1 AND their_id IN `
	removeIdentityKeysQuery   = `DELETE FROM whatsmeow_identity_keys WHERE our_jid=@p1 AND their_id IN `
	mssqlPutNodeWithGroup     = `INSERT INTO whatsapp_message_node (our_jid, their_jid, group_id, node, insert_timestamp) VALUES (@p1, @p2, @p3, @p4, @p5)`
	mssqlPutNodeWithoutGroup  = `INSERT INTO whatsapp_message_node (our_jid, their_jid, node, insert_timestamp) VALUES (@p1, @p2, @p3, @p4)`
	getNodesByGroup           = `SELECT id, node FROM whatsapp_message_node WITH (NOLOCK) WHERE our_jid=@p1 AND group_id=@p2`
	getNodesByPerson          = `SELECT id, node FROM whatsapp_message_node WITH (NOLOCK) WHERE our_jid=@p1 AND their_jid=@p2`
	removeMessageNodeQuery    = `DELETE FROM whatsapp_message_node WHERE id=@p1`
	removeOldMessageNodeQuery = `DELETE FROM whatsapp_message_node WHERE insert_timestamp < @p1`
)

func (s *SQLStore) CacheSessions(ctx context.Context, addresses []string) (final map[string][]byte) {
	final = make(map[string][]byte)
	if len(addresses) == 0 {
		return final
	}
	query := getCacheSessionQuery + "("
	queryParams := make([]interface{}, len(addresses)+1)
	queryParams[0] = s.JID
	for index, address := range addresses {
		if index > 0 {
			query += ","
		}
		query += fmt.Sprintf("@p%d", index+2)
		queryParams[index+1] = address
	}
	query += ")"
	rows, err := s.db.Query(ctx, query, queryParams...)
	if err != nil {
		s.log.Errorf(err.Error())
		return
	}
	for rows.Next() {
		var session []byte
		var id string
		rows.Scan(&id, &session)
		final[id] = session
	}
	return
}

func (s *SQLStore) CacheIdentities(ctx context.Context, addresses []string) (final map[string][32]byte) {
	final = make(map[string][32]byte)
	if len(addresses) == 0 {
		return final
	}
	query := getCacheIdentityQuery + "("
	queryParams := make([]interface{}, len(addresses)+1)
	queryParams[0] = s.JID
	for index, address := range addresses {
		if index > 0 {
			query += ","
		}
		query += fmt.Sprintf("@p%d", index+2)
		queryParams[index+1] = address
	}
	query += ")"
	rows, err := s.db.Query(ctx, query, queryParams...)
	if err != nil {
		s.log.Errorf(err.Error())
		return
	}
	for rows.Next() {
		var session []byte
		var id string
		rows.Scan(&id, &session)
		final[id] = *(*[32]byte)(session)
	}
	return
}

func (s *SQLStore) StoreSessions(ctx context.Context, sessions map[string][]byte, oldAddresses []string) {
	s.db.DoTxn(ctx, nil, func(ctx context.Context) error {
		if len(oldAddresses) > 0 {
			query := removeSessionsQuery + "("
			queryParams := make([]interface{}, len(oldAddresses)+1)
			queryParams[0] = s.JID
			for index, address := range oldAddresses {
				if index > 0 {
					query += ","
				}
				query += fmt.Sprintf("@p%d", index+2)
				queryParams[index+1] = address
			}
			query += ")"
			_, err := s.db.Exec(ctx, query, queryParams...)
			if err != nil {
				s.log.Errorf("Could not Remove Sessions: " + err.Error())
				return err
			}
		}
		dt := time.Now()
		_, err := s.db.Exec(ctx, fmt.Sprintf(`CREATE TABLE staging_sessions_%d (
			our_jid   VARCHAR(300),
			their_id  VARCHAR(300),
			session  VARBINARY(max),
		)`, dt.UnixMilli()))
		if err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
		bulkImportStr := mssql.CopyIn(fmt.Sprintf("staging_sessions_%d", dt.UnixMilli()), mssql.BulkOptions{}, "our_jid", "their_id", "session")
		stmt, err := s.db.PrepareContext(ctx, bulkImportStr)
		if err != nil {
			s.log.Errorf("Could not Prepare Statement: " + err.Error())
			return err
		}
		for address, session := range sessions {
			stmt.Exec(s.JID, address, session[:])
		}
		_, err = stmt.Exec()
		if err != nil {
			s.log.Errorf("Could not Store Sessions: " + err.Error())
			return err
		}
		_, err = s.db.Exec(ctx, fmt.Sprintf(`MERGE INTO whatsmeow_sessions AS target 
			USING staging_sessions_%d AS source 
			ON target.our_jid = source.our_jid AND target.their_id = source.their_id
			WHEN MATCHED THEN
				UPDATE SET target.session = source.session 
			WHEN NOT MATCHED THEN 
				INSERT (our_jid, their_id, session) 
				VALUES (source.our_jid, source.their_id, source.session);`, dt.UnixMilli()))
		if err != nil {
			s.log.Errorf("failed to merge bulk: " + err.Error())
			return fmt.Errorf("failed to merge bulk: %w", err)
		}
		_, err = s.db.Exec(ctx, fmt.Sprintf("DROP TABLE staging_sessions_%d", dt.UnixMilli()))
		if err != nil {
			s.log.Errorf("failed to drop table: " + err.Error())
			return fmt.Errorf("failed to drop table: %w", err)
		}
		return nil
	})
}

func (s *SQLStore) StoreIdentities(ctx context.Context, identityKeys map[string][32]byte, oldAddresses []string) {
	s.db.DoTxn(ctx, nil, func(ctx context.Context) error {
		if len(oldAddresses) > 0 {
			query := removeIdentityKeysQuery + "("
			queryParams := make([]interface{}, len(oldAddresses)+1)
			queryParams[0] = s.JID
			for index, address := range oldAddresses {
				if index > 0 {
					query += ","
				}
				query += fmt.Sprintf("@p%d", index+2)
				queryParams[index+1] = address
			}
			query += ")"
			_, err := s.db.Exec(ctx, query, queryParams...)
			if err != nil {
				s.log.Errorf("Could not Remove Identity Keys: " + err.Error())
				return err
			}
		}
		dt := time.Now()
		_, err := s.db.Exec(ctx, fmt.Sprintf(`CREATE TABLE staging_identitykeys_%d (
			our_jid  VARCHAR(300),
			their_id VARCHAR(300),
			identity_info VARBINARY(max) NOT NULL CHECK ( LEN(identity_info) = 32 ),
		)`, dt.UnixMilli()))
		if err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
		bulkImportStr := mssql.CopyIn(fmt.Sprintf("staging_identitykeys_%d", dt.UnixMilli()), mssql.BulkOptions{}, "our_jid", "their_id", "identity_info")
		stmt, err := s.db.PrepareContext(ctx, bulkImportStr)
		if err != nil {
			s.log.Errorf("Could not Prepare Statement: " + err.Error())
			return err
		}
		for address, identityInfo := range identityKeys {
			stmt.Exec(s.JID, address, identityInfo[:])
		}
		_, err = stmt.Exec()
		if err != nil {
			s.log.Errorf("Could not Store Identity Keys: " + err.Error())
			return err
		}
		_, err = s.db.Exec(ctx, fmt.Sprintf(`MERGE INTO whatsmeow_identity_keys AS target
			USING staging_identitykeys_%d AS source
			ON target.our_jid = source.our_jid AND target.their_id = source.their_id
			WHEN MATCHED THEN
				UPDATE SET target.identity_info = source.identity_info
			WHEN NOT MATCHED THEN
				INSERT (our_jid, their_id, identity_info)
				VALUES (source.our_jid, source.their_id, source.identity_info);`, dt.UnixMilli()))
		if err != nil {
			s.log.Errorf("failed to merge bulk: " + err.Error())
			return fmt.Errorf("failed to merge bulk: %w", err)
		}
		_, err = s.db.Exec(ctx, fmt.Sprintf("DROP TABLE staging_identitykeys_%d", dt.UnixMilli()))
		if err != nil {
			s.log.Errorf("failed to drop table: " + err.Error())
			return fmt.Errorf("failed to drop table: %w", err)
		}
		return nil
	})
}

func (s *SQLStore) PutMessageNode(ctx context.Context, user string, group *string, node *waBinary.Node) (err error) {
	nodeData, err := json.Marshal(node)
	if err != nil {
		return fmt.Errorf("failed to marshal node: %w", err)
	}
	if group != nil {
		if s.db.Dialect == dbutil.MSSQL {
			_, err = s.db.Exec(ctx, mssqlPutNodeWithGroup, s.JID, user, *group, nodeData, time.Now().UnixMilli())
		}
	} else {
		if s.db.Dialect == dbutil.MSSQL {
			_, err = s.db.Exec(ctx, mssqlPutNodeWithoutGroup, s.JID, user, nodeData, time.Now().UnixMilli())
		}
	}
	return err
}
func (s *SQLStore) GetMessageNodesByUser(ctx context.Context, user string) (map[int]waBinary.Node, error) {
	rows, err := s.db.Query(ctx, getNodesByPerson, s.JID, user)
	if err != nil {
		return nil, err
	}
	output := map[int]waBinary.Node{}
	defer rows.Close()
	for rows.Next() {
		var ref int
		var bytes []byte
		err = rows.Scan(&ref, &bytes)
		if err != nil {
			return nil, fmt.Errorf("error scanning row: %w", err)
		}
		node := waBinary.Node{}
		node.UnmarshalJSON(bytes)
		output[ref] = node
	}
	return output, nil
}
func (s *SQLStore) GetMessageNodesByGroup(ctx context.Context, group string) (map[int]waBinary.Node, error) {
	rows, err := s.db.Query(ctx, getNodesByGroup, s.JID, group)
	if err != nil {
		return nil, err
	}
	output := map[int]waBinary.Node{}
	defer rows.Close()
	for rows.Next() {
		var ref int
		var bytes []byte
		err = rows.Scan(&ref, &bytes)
		if err != nil {
			return nil, fmt.Errorf("error scanning row: %w", err)
		}
		node := waBinary.Node{}
		node.UnmarshalJSON(bytes)
		output[ref] = node
	}
	return output, nil
}

func (s *SQLStore) DeleteMessageNode(ctx context.Context, ref int) (err error) {
	_, err = s.db.Exec(ctx, removeMessageNodeQuery, ref)
	return err
}
func (s *SQLStore) DeleteOldMessageNodes(ctx context.Context) (err error) {
	_, err = s.db.Exec(ctx, removeOldMessageNodeQuery, time.Now().Add(-14*24*time.Hour).UnixMilli())
	return err
}

const (
	sqliteGetBufferedEventQuery = `
		SELECT plaintext, server_timestamp, insert_timestamp FROM whatsmeow_event_buffer WHERE our_jid = $1 AND ciphertext_hash = $2
	`
	mssqlGetBufferedEventQuery = `
		SELECT plaintext, server_timestamp, insert_timestamp FROM whatsmeow_event_buffer WHERE our_jid = @p1 AND ciphertext_hash = @p2
	`
	sqlitePutBufferedEventQuery = `
		INSERT INTO whatsmeow_event_buffer (our_jid, ciphertext_hash, plaintext, server_timestamp, insert_timestamp)
		VALUES ($1, $2, $3, $4, $5)
	`
	mssqlPutBufferedEventQuery = `
		INSERT INTO whatsmeow_event_buffer (our_jid, ciphertext_hash, plaintext, server_timestamp, insert_timestamp)
		VALUES (@p1, @p2, @p3, @p4, @p5)
	`
	sqliteClearBufferedEventPlaintextQuery = `
		UPDATE whatsmeow_event_buffer SET plaintext = NULL WHERE our_jid = $1 AND ciphertext_hash = $2
	`
	mssqlClearBufferedEventPlaintextQuery = `
		UPDATE whatsmeow_event_buffer SET plaintext = NULL WHERE our_jid = @p1 AND ciphertext_hash = @p2
	`
	sqliteDeleteOldBufferedHashesQuery = `
		DELETE FROM whatsmeow_event_buffer WHERE insert_timestamp < $1
	`
	mssqlDeleteOldBufferedHashesQuery = `
		DELETE FROM whatsmeow_event_buffer WHERE insert_timestamp < @p1
	`
)

func (s *SQLStore) GetBufferedEvent(ctx context.Context, ciphertextHash [32]byte) (*store.BufferedEvent, error) {
	var insertTimeMS, serverTimeSeconds int64
	var buf store.BufferedEvent
	var err error
	if s.db.Dialect == dbutil.MSSQL {
		err = s.db.QueryRow(ctx, mssqlGetBufferedEventQuery, s.JID, ciphertextHash[:]).Scan(&buf.Plaintext, &serverTimeSeconds, &insertTimeMS)
	} else {
		err = s.db.QueryRow(ctx, sqliteGetBufferedEventQuery, s.JID, ciphertextHash[:]).Scan(&buf.Plaintext, &serverTimeSeconds, &insertTimeMS)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	buf.ServerTime = time.Unix(serverTimeSeconds, 0)
	buf.InsertTime = time.UnixMilli(insertTimeMS)
	return &buf, nil
}

func (s *SQLStore) PutBufferedEvent(ctx context.Context, ciphertextHash [32]byte, plaintext []byte, serverTimestamp time.Time) error {
	if s.db.Dialect == dbutil.MSSQL {
		_, err := s.db.Exec(ctx, mssqlPutBufferedEventQuery, s.JID, ciphertextHash[:], plaintext, serverTimestamp.Unix(), time.Now().UnixMilli())
		return err
	}
	_, err := s.db.Exec(ctx, sqlitePutBufferedEventQuery, s.JID, ciphertextHash[:], plaintext, serverTimestamp.Unix(), time.Now().UnixMilli())
	return err
}

func (s *SQLStore) DoDecryptionTxn(ctx context.Context, fn func(context.Context) error) error {
	ctx = context.WithValue(ctx, dbutil.ContextKeyDoTxnCallerSkip, 2)
	return s.db.DoTxn(ctx, nil, fn)
}

func (s *SQLStore) ClearBufferedEventPlaintext(ctx context.Context, ciphertextHash [32]byte) error {
	if s.db.Dialect == dbutil.MSSQL {
		_, err := s.db.Exec(ctx, mssqlClearBufferedEventPlaintextQuery, s.JID, ciphertextHash[:])
		return err
	}
	_, err := s.db.Exec(ctx, sqliteClearBufferedEventPlaintextQuery, s.JID, ciphertextHash[:])
	return err
}

func (s *SQLStore) DeleteOldBufferedHashes(ctx context.Context) error {
	// The WhatsApp servers only buffer events for 14 days,
	// so we can safely delete anything older than that.
	if s.db.Dialect == dbutil.MSSQL {
		_, err := s.db.Exec(ctx, mssqlDeleteOldBufferedHashesQuery, time.Now().Add(-14*24*time.Hour).UnixMilli())
		return err
	}
	_, err := s.db.Exec(ctx, sqliteDeleteOldBufferedHashesQuery, time.Now().Add(-14*24*time.Hour).UnixMilli())
	return err
}
