// Copyright (c) 2022 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	mathRand "math/rand/v2"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.mau.fi/util/dbutil"
	"go.mau.fi/util/random"

	"go.mau.fi/whatsmeow/proto/waAdv"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore/upgrades"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/util/keys"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// Container is a wrapper for a SQL database that can contain multiple whatsmeow sessions.
type Container struct {
	db     *RetryDB
	log    waLog.Logger
	mutex  sync.Mutex
	LIDMap *CachedLIDMap
}

var _ store.DeviceContainer = (*Container)(nil)

var dbInstance *sql.DB

// New connects to the given SQL database and wraps it in a Container.
//
// Only SQLite and Postgres are currently fully supported.
//
// The logger can be nil and will default to a no-op logger.
//
// When using SQLite, it's strongly recommended to enable foreign keys by adding `?_foreign_keys=true`:
//
//	container, err := sqlstore.New(context.Background(), "sqlite3", "file:yoursqlitefile.db?_foreign_keys=on", nil)
func New(ctx context.Context, dialect, address string, log waLog.Logger, dbLogger dbutil.DatabaseLogger, maxConnection int) (*Container, error) {
	if dbInstance == nil {
		db, err := sql.Open(dialect, address)
		if err != nil {
			return nil, fmt.Errorf("failed to open database: %w", err)
		}
		db.SetMaxOpenConns(maxConnection)
		db.SetMaxIdleConns(maxConnection)
		dbInstance = db
	}
	container := NewWithDB(dbInstance, dialect, log, dbLogger)
	err := container.Upgrade(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to upgrade database: %w", err)
	}
	return container, nil
}

// NewWithDB wraps an existing SQL connection in a Container.
//
// Only SQLite and Postgres are currently fully supported.
//
// The logger can be nil and will default to a no-op logger.
//
// When using SQLite, it's strongly recommended to enable foreign keys by adding `?_foreign_keys=true`:
//
//	db, err := sql.Open("sqlite3", "file:yoursqlitefile.db?_foreign_keys=on")
//	if err != nil {
//	    panic(err)
//	}
//	container := sqlstore.NewWithDB(db, "sqlite3", nil)
//
// This method does not call Upgrade automatically like New does, so you must call it yourself:
//
//	container := sqlstore.NewWithDB(...)
//	err := container.Upgrade()
func NewWithDB(db *sql.DB, dialect string, log waLog.Logger, dbLogger dbutil.DatabaseLogger) *Container {
	dbutil.NoopLogger = dbLogger
	wrapped, err := dbutil.NewWithDB(db, dialect)
	if err != nil {
		// This will only panic if the dialect is invalid
		panic(err)
	}
	wrapped.UpgradeTable = upgrades.Table
	wrapped.VersionTable = "whatsmeow_version"
	return NewWithWrappedDB(&RetryDB{
		Database: wrapped,
	}, log)
}

func NewWithWrappedDB(wrapped *RetryDB, log waLog.Logger) *Container {
	if log == nil {
		log = waLog.Noop
	}
	return &Container{
		db:     wrapped,
		log:    log,
		LIDMap: NewCachedLIDMap(wrapped),
	}
}

// Upgrade upgrades the database from the current to the latest version available.
func (c *Container) Upgrade(ctx context.Context) error {
	if c.db.Dialect == dbutil.SQLite {
		var foreignKeysEnabled bool
		err := c.db.QueryRow(ctx, "PRAGMA foreign_keys").Scan(&foreignKeysEnabled)
		if err != nil {
			return fmt.Errorf("failed to check if foreign keys are enabled: %w", err)
		} else if !foreignKeysEnabled {
			return fmt.Errorf("foreign keys are not enabled")
		}
	}

	return c.db.Upgrade(ctx)
}

const getAllDevicesQuery = `
SELECT jid, lid, registration_id, noise_key, identity_key,
       signed_pre_key, signed_pre_key_id, signed_pre_key_sig,
       adv_key, adv_details, adv_account_sig, adv_account_sig_key, adv_device_sig,
       platform, business_name, push_name, facebook_uuid, manager_id
FROM whatsmeow_device
`

const lockDeviceByManagerId = `UPDATE whatsmeow_device SET locktime = @p1 WHERE manager_id = @p2 AND locktime<@p3`

const relockDeviceByManagerId = `UPDATE whatsmeow_device SET locktime = @p1 WHERE manager_id = @p2 AND locktime = @p3`

const unlockDeviceByManagerId = `UPDATE whatsmeow_device SET locktime = 0 WHERE manager_id = @p1 AND locktime = @p2`

const checkActiveDeviceManagerId = `SELECT 1 FROM whatsmeow_device WHERE manager_id = @p1 AND locktime >= @p2`

const getDeviceByManagerId = `SELECT jid, lid, registration_id, noise_key, identity_key,
signed_pre_key, signed_pre_key_id, signed_pre_key_sig,
adv_key, adv_details, adv_account_sig, adv_account_sig_key, adv_device_sig,
platform, business_name, push_name, facebook_uuid, manager_id, locktime
FROM whatsmeow_device
WHERE manager_id = @p1 AND locktime = @p2`

const getDeviceByManagerIdNoLock = `SELECT jid, lid, registration_id, noise_key, identity_key,
signed_pre_key, signed_pre_key_id, signed_pre_key_sig,
adv_key, adv_details, adv_account_sig, adv_account_sig_key, adv_device_sig,
platform, business_name, push_name, facebook_uuid, manager_id, locktime
FROM whatsmeow_device
WHERE manager_id = @p1`

const existsLockedDevice = `SELECT TOP 1 1 FROM whatsmeow_device WHERE manager_id = @p1 AND locktime >= @p2`

const getActiveManagerIds = `SELECT manager_id FROM whatsmeow_device GROUP BY manager_id`

const getNumberManager = `SELECT TOP 1 manager_id FROM whatsmeow_device WHERE jid=@p1`

const getDeviceQuery = getAllDevicesQuery + " WHERE jid=@p1"

func (c *Container) GetActiveManagers() ([]string, error) {
	// conn, err := c.db.Conn(ctx)
	// if err != nil {
	// 	return nil, err
	// }
	// defer conn.Close()
	// rows, err := conn.QueryContext(ctx, getActiveManagerIds)
	rows, err := c.db.RawDB.Query(getActiveManagerIds)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []string

	for rows.Next() {
		var value string
		err := rows.Scan(&value)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, err
}


func (c *Container) GetNumberManager(jid types.JID) (string, error) {
	var managerId string
	err := c.db.RawDB.QueryRow(getNumberManager, jid).Scan(&managerId)
	if err != nil {
		return "", err
	}
	return managerId, nil
}


func (c *Container) GetManagerFirstDevice(managerId string) (*store.Device, error) {
	devices, _, err := c.GetAllManagerDevice(managerId, 0)
	unlockerror := c.UnlockManagerDevice(managerId, 0)
	if unlockerror != nil {
		c.log.Errorf("Unlock device error: %s", unlockerror.Error())
	}
	if err != nil {
		return nil, err
	}
	if len(devices) == 0 {
		return c.NewDevice(managerId), nil
	} else {
		return devices[0], nil
	}
}

func (c *Container) scanDevice(row dbutil.Scannable) (*store.Device, error) {
	var device store.Device
	device.Log = c.log
	device.SignedPreKey = &keys.PreKey{}
	var noisePriv, identityPriv, preKeyPriv, preKeySig []byte
	var account waAdv.ADVSignedDeviceIdentity
	var fbUUID uuid.NullUUID

	err := row.Scan(
		&device.ID, &device.LID, &device.RegistrationID, &noisePriv, &identityPriv,
		&preKeyPriv, &device.SignedPreKey.KeyID, &preKeySig,
		&device.AdvSecretKey, &account.Details, &account.AccountSignature, &account.AccountSignatureKey, &account.DeviceSignature,
		&device.Platform, &device.BusinessName, &device.PushName, &fbUUID, &device.ManagerId, &device.LockTime)
	if err != nil {
		return nil, fmt.Errorf("failed to scan session: %w", err)
	} else if len(noisePriv) != 32 || len(identityPriv) != 32 || len(preKeyPriv) != 32 || len(preKeySig) != 64 {
		return nil, ErrInvalidLength
	}

	device.NoiseKey = keys.NewKeyPairFromPrivateKey(*(*[32]byte)(noisePriv))
	device.IdentityKey = keys.NewKeyPairFromPrivateKey(*(*[32]byte)(identityPriv))
	device.SignedPreKey.KeyPair = *keys.NewKeyPairFromPrivateKey(*(*[32]byte)(preKeyPriv))
	device.SignedPreKey.Signature = (*[64]byte)(preKeySig)
	device.Account = &account
	device.FacebookUUID = fbUUID.UUID

	c.initializeDevice(&device)

	return &device, nil
}

// GetAllDevices finds all the devices in the database.
func (c *Container) GetAllDevices(ctx context.Context) ([]*store.Device, error) {
	res, err := c.db.Query(ctx, getAllDevicesQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query sessions: %w", err)
	}
	defer res.Close()
	sessions := make([]*store.Device, 0)
	for res.Next() {
		sess, scanErr := c.scanDevice(res)
		if scanErr != nil {
			return sessions, scanErr
		}
		sessions = append(sessions, sess)
	}
	return sessions, nil
}

// GetFirstDevice is a convenience method for getting the first device in the store. If there are
// no devices, then a new device will be created. You should only use this if you don't want to
// have multiple sessions simultaneously.
func (c *Container) GetFirstDevice(ctx context.Context, managerId string) (*store.Device, error) {
	devices, err := c.GetAllDevices(ctx)
	if err != nil {
		return nil, err
	}
	if len(devices) == 0 {
		return c.NewDevice(managerId), nil
	} else {
		return devices[0], nil
	}
}

func (c *Container) GetAllManagerDevice(managerId string, lockTime int) ([]*store.Device, int64, error) {
	dt := time.Now().Add(time.Duration(lockTime) * time.Minute).UnixMilli()
	mintime := time.Now().UnixMilli()
	_, err := c.db.RawDB.Exec(lockDeviceByManagerId, dt, managerId, mintime)
	if err != nil {
		return nil, 0, err
	}
	rows, err := c.db.RawDB.Query(getDeviceByManagerId, managerId, dt)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var devices []*store.Device
	for rows.Next() {
		device, err := c.scanDevice(rows)
		if err != nil {
			return nil, 0, err
		}
		devices = append(devices, device)
	}
	return devices, dt, nil
}

func (c *Container) ReLockManagerDevice(managerId string, lastLockedTime, newLockedTime int64) (bool, error) {
	res, err := c.db.RawDB.Exec(relockDeviceByManagerId, newLockedTime, managerId, lastLockedTime)
	if err != nil {
		return false, err
	}
	cnt, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return cnt > 0, nil
}

func (c *Container) GetAllManagerDeviceWithoutLock(managerId string) ([]*store.Device, error) {
	rows, err := c.db.RawDB.Query(getDeviceByManagerIdNoLock, managerId)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var devices []*store.Device
	for rows.Next() {
		device, err := c.scanDevice(rows)
		if err != nil {
			return nil, err
		}
		devices = append(devices, device)
	}
	return devices, nil
}

func (c *Container) ExistsLockedDevice(managerId string) bool {
	dt := time.Now().UnixMilli()
	var exists bool
	err := c.db.RawDB.QueryRow(existsLockedDevice, managerId, dt).Scan(&exists)
	if err != nil {
		return false
	}
	return exists
}

func (c *Container) UnlockManagerDevice(managerId string, lastLock int64) error {
	_, err := c.db.RawDB.Exec(unlockDeviceByManagerId, managerId, lastLock)
	return err
}

// GetDevice finds the device with the specified JID in the database.
//
// If the device is not found, nil is returned instead.
//
// Note that the parameter usually must be an AD-JID.
func (c *Container) GetDevice(ctx context.Context, jid types.JID) (*store.Device, error) {
	sess, err := c.scanDevice(c.db.QueryRow(ctx, getDeviceQuery, jid))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return sess, err
}

const (
	sqliteInsertDeviceQuery = `
		INSERT INTO whatsmeow_device (jid, lid, registration_id, noise_key, identity_key,
									  signed_pre_key, signed_pre_key_id, signed_pre_key_sig,
									  adv_key, adv_details, adv_account_sig, adv_account_sig_key, adv_device_sig,
									  platform, business_name, push_name, facebook_uuid, manager_id)
		VALUES (@p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8, @p9, @p10, @p11, @p12, @p13, @p14, @p15, @p16, @p17, @p18)
		ON CONFLICT (jid) DO UPDATE
			SET lid=excluded.lid,
				platform=excluded.platform,
				business_name=excluded.business_name,
				push_name=excluded.push_name
	`
	mssqlInsertDeviceQuery = `
		MERGE INTO whatsmeow_device AS target
		USING (VALUES (@p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8, @p9, @p10, @p11, @p12, @p13, @p14, @p15, @p16, @p17, @p18)) AS source (jid, lid, registration_id, noise_key, identity_key, signed_pre_key, signed_pre_key_id, signed_pre_key_sig, adv_key, adv_details, adv_account_sig, adv_account_sig_key, adv_device_sig, platform, business_name, push_name, facebook_uuid, manager_id)
		ON (target.jid = source.jid)
		WHEN MATCHED THEN
			UPDATE SET target.lid = source.lid, target.platform = source.platform, target.business_name = source.business_name, target.push_name = source.push_name
		WHEN NOT MATCHED THEN
			INSERT (jid, lid, registration_id, noise_key, identity_key, signed_pre_key, signed_pre_key_id, signed_pre_key_sig, adv_key, adv_details, adv_account_sig, adv_account_sig_key, adv_device_sig, platform, business_name, push_name, facebook_uuid, manager_id)
			VALUES (source.jid, source.lid, source.registration_id, CONVERT(varbinary(max),source.noise_key), source.identity_key, source.signed_pre_key, source.signed_pre_key_id, source.signed_pre_key_sig, source.adv_key, source.adv_details, source.adv_account_sig, source.adv_account_sig_key, source.adv_device_sig, source.platform, source.business_name, source.push_name, source.facebook_uuid, source.manager_id);
	`
	deleteDeviceQuery  = `DELETE FROM whatsmeow_device WHERE jid=@p1`
	deleteMessageNodes = `DELETE FROM whatsapp_message_node WHERE our_jid=@p1`
)

// NewDevice creates a new device in this database.
//
// No data is actually stored before Save is called. However, the pairing process will automatically
// call Save after a successful pairing, so you most likely don't need to call it yourself.
func (c *Container) NewDevice(managerId string) *store.Device {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	innerStore := NewSQLStore(c, types.EmptyJID)
	device := &store.Device{
		Log:       c.log,
		Container: c,

		ManagerId:      managerId,
		NoiseKey:       keys.NewKeyPair(),
		IdentityKey:    keys.NewKeyPair(),
		RegistrationID: mathRand.Uint32(),
		AdvSecretKey:   random.Bytes(32),
	}
	device.SignedPreKey = device.IdentityKey.CreateSignedPreKey(1)
	device.Identities = innerStore
	device.Sessions = innerStore
	device.PreKeys = innerStore
	device.SenderKeys = innerStore
	device.AppStateKeys = innerStore
	device.AppState = innerStore
	device.Contacts = innerStore
	device.ChatSettings = innerStore
	device.MsgSecrets = innerStore
	device.PrivacyTokens = innerStore
	device.PrekeysCache = innerStore
	return device
}

// ErrDeviceIDMustBeSet is the error returned by PutDevice if you try to save a device before knowing its JID.
var ErrDeviceIDMustBeSet = errors.New("device JID must be known before accessing database")

// Close will close the container's database
func (c *Container) Close() error {
	if c != nil && c.db != nil {
		return c.db.Close()
	}
	return nil
}

// PutDevice stores the given device in this database. This should be called through Device.Save()
// (which usually doesn't need to be called manually, as the library does that automatically when relevant).
func (c *Container) PutDevice(ctx context.Context, device *store.Device) error {
	if device.ID == nil {
		return ErrDeviceIDMustBeSet
	}
	var err error
	if c.db.Dialect == dbutil.MSSQL {
		_, err = c.db.Exec(ctx, mssqlInsertDeviceQuery,
			device.ID.String(), device.LID, device.RegistrationID, device.NoiseKey.Priv[:], device.IdentityKey.Priv[:],
			device.SignedPreKey.Priv[:], device.SignedPreKey.KeyID, device.SignedPreKey.Signature[:],
			device.AdvSecretKey, device.Account.Details, device.Account.AccountSignature, device.Account.AccountSignatureKey, device.Account.DeviceSignature,
			device.Platform, device.BusinessName, device.PushName, device.FacebookUUID.String(), device.ManagerId)
	} else {
		_, err = c.db.Exec(ctx, sqliteInsertDeviceQuery,
			device.ID.String(), device.LID, device.RegistrationID, device.NoiseKey.Priv[:], device.IdentityKey.Priv[:],
			device.SignedPreKey.Priv[:], device.SignedPreKey.KeyID, device.SignedPreKey.Signature[:],
			device.AdvSecretKey, device.Account.Details, device.Account.AccountSignature, device.Account.AccountSignatureKey, device.Account.DeviceSignature,
			device.Platform, device.BusinessName, device.PushName, uuid.NullUUID{UUID: device.FacebookUUID, Valid: device.FacebookUUID != uuid.Nil}, device.ManagerId)
	}
	if !device.Initialized {
		c.initializeDevice(device)
	}
	return err
}

func (c *Container) initializeDevice(device *store.Device) {
	innerStore := NewSQLStore(c, *device.ID)
	device.Identities = innerStore
	device.Sessions = innerStore
	device.PreKeys = innerStore
	device.SenderKeys = innerStore
	device.AppStateKeys = innerStore
	device.AppState = innerStore
	device.Contacts = innerStore
	device.ChatSettings = innerStore
	device.MsgSecrets = innerStore
	device.PrivacyTokens = innerStore
	device.PrekeysCache = innerStore
	device.EventBuffer = innerStore
	device.LIDs = c.LIDMap
	device.Container = c
	device.Initialized = true
}

// DeleteDevice deletes the given device from this database. This should be called through Device.Delete()
func (c *Container) DeleteDevice(ctx context.Context, store *store.Device) error {
	if store.ID == nil {
		return ErrDeviceIDMustBeSet
	}
	_, err := c.db.Exec(ctx, deleteDeviceQuery, store.ID)
	return err
}

func (c *Container) DeleteMessageNode(ctx context.Context, store *store.Device) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if store.ID == nil {
		return ErrDeviceIDMustBeSet
	}
	_, err := c.db.RawDB.Exec(deleteMessageNodes, store.ID.String())
	return err
}
