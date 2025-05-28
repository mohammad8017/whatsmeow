-- v8 (compatible with v8+): Add tables for LID<->JID mapping
CREATE TABLE whatsmeow_lid_map (
    lid NVARCHAR(300) PRIMARY KEY,
    pn NVARCHAR(300) UNIQUE NOT NULL
);
