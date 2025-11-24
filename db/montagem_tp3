DROP TABLE IF EXISTS projeto_de_bloco_tp3.montagem CASCADE;

CREATE TABLE projeto_de_bloco_tp3.montagem (
    id_montagem UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    xid_cliente UUID REFERENCES projeto_de_bloco_tp3.clientes(id_cliente),
    placa_mae VARCHAR(50) NOT NULL,
    fonte VARCHAR(50) NOT NULL,
    armazenamento1 VARCHAR(50) NOT NULL,
    armazenamento2 VARCHAR(50),
    processador VARCHAR(50) NOT NULL,
    memoria_ram VARCHAR(50) NOT NULL,
    placa_video VARCHAR(50) NOT NULL,
    data_compra DATE  NOT NULL,
    total NUMERIC(10, 2) NOT NULL,
    rgb BOOLEAN NOT NULL,
    loja_preferencia VARCHAR(50) NOT NULL,
    water_cooler BOOLEAN NOT NULL
);

INSERT INTO projeto_de_bloco_tp3.montagem (
    id_montagem,
    xid_cliente,
    placa_mae,
    fonte,
    armazenamento1,
    armazenamento2,
    processador,
    memoria_ram,
    placa_video,
    data_compra,
    total,
    rgb,
    loja_preferencia,
    water_cooler
)
VALUES
('87af1cdd-760d-4cd1-99ef-9bce6ff878a8',
 '60df8cd4-2b32-429c-ba8b-73eb62147b3b',
 'ASUS B550M-PLUS','Corsair CX550','SSD NVMe 500GB','HD 1TB',
 'Ryzen 5 5600','16GB DDR4 3200','RTX 3060','2025-01-10',4300,
 TRUE,'Kabum',FALSE),

('53308fb1-5897-47de-b4ef-34134820a3c7',
 'a6080990-9c2d-45e2-b214-009c382894bc',
 'Gigabyte H510M-H','EVGA 500W White','SSD 480GB','HD 1TB',
 'Intel i5-10400','16GB DDR4 2666','GTX 1660 Super','2025-02-03',3500,
 FALSE,'Pichau',FALSE),

('db175cdb-dd5e-4221-be9f-1d93bfbedd68',
 'a92cacbe-5f85-4823-b5c5-a6d643e518e0',
 'MSI B760 Tomahawk','Corsair RM650','SSD NVMe 1TB','SSD NVMe 1TB',
 'Intel i5-14400F','32GB DDR5 6000','RTX 4070','2025-01-28',9000,
 TRUE,'Terabyteshop',TRUE),

('b4493a51-b4fc-4da8-8d06-1b1d75fb5e27',
 '06573745-f198-4f30-a977-e954555356d4',
 'ASRock A320M-HD','PCYes 450W','SSD 240GB',NULL,
 'Ryzen 3 2200G','8GB DDR4 2400','Integrada Vega 8','2024-12-19',1800,
 FALSE,'Custom',FALSE),

('70414f1b-d7e7-4dbe-8b6f-d3da80376ff5',
 'b05cf27a-e9c0-4426-834b-8b6509734d22',
 'ASUS Z790-A','Corsair RM850x','SSD NVMe 2TB','SSD NVMe 1TB',
 'Intel i7-14700K','32GB DDR5 7200','RTX 4080 Super','2025-03-12',15000,
 TRUE,'Kabum',TRUE),

('4cf01890-6f72-4198-9c61-a5c8c42d11b3',
 '562e9006-4fc8-46c2-b07b-58dd4b6fad58',
 'Gigabyte B450M DS3H','CoolerMaster 550W','SSD 480GB','HD 1TB',
 'Ryzen 5 3600','16GB DDR4 3000','RX 6700 XT','2025-01-02',5200,
 TRUE,'Pichau',FALSE),

('e8277772-b4e9-4504-9af3-1264afa04612',
 '6e639b0d-9fc1-4aa9-b870-f4be3e750807',
 'ASRock H97 Pro4','Corsair CX500','HD 1TB',NULL,
 'Intel i7-4790','16GB DDR3 1600','GTX 970','2024-11-07',2100,
 FALSE,'Studio PC',FALSE),

('6db3edd3-7d69-4d53-af23-b3f66b7a1653',
 '46582146-a525-4953-a476-f25b0983dd6e',
 'MSI X570 Gaming Edge','Corsair RM750','SSD NVMe 1TB','HD 2TB',
 'Ryzen 7 5800X','32GB DDR4 3600','RTX 3080','2025-02-19',8200,
 TRUE,'Terabyteshop',TRUE),

('27ff37d3-21f0-4813-9ce1-4374cacd4c90',
 '9c5d2191-abdf-44d7-997f-ed7e211e9341',
 'ASUS B660M-PLUS','EVGA 600W','SSD NVMe 1TB',NULL,
 'Intel i5-12400F','16GB DDR4 3200','RTX 3060 Ti','2025-03-01',5300,
 TRUE,'Kabum',FALSE),

('d293ac8d-9b3c-419c-8998-57bb66d7e126',
 '390b24c6-5cea-42aa-b374-db25b63a46f0',
 'Gigabyte GA-970A-DS3','PCYes 500W','HD 500GB',NULL,
 'FX-8350','8GB DDR3 1600','R9 380','2024-10-28',1500,
 FALSE,'Custom',FALSE),

('f190e94b-5c71-47dd-9fbe-179c96638ae1',
 'cd74a80b-2112-4af9-aa7b-e2d5fdb1a150',
 'Asus TUF GAMING B550M-PLUS WIFI II','XPG Kyber 850W 80 Plus Gold',
 'SSD NVMe M.2 1TB Kingston (principal)','SSD NVMe M.2 1TB (secundário)',
 'Ryzen 7 5800X','32GB DDR4 3600 MHz JUHOR',
 'ASUS Prime GeForce RTX 5070 OC Edition 12GB','2025-03-20',11000,
 TRUE,'Pichau',TRUE),

('93663ac8-2815-4fdd-b8c5-8408f29d4bda',
 '721b428f-4e1c-49b3-8ac6-226a4425d729',
 'ASUS PRIME B450M-A','Corsair CX450','SSD 240GB',NULL,
 'Ryzen 3 3200G','16GB DDR4 2666','Integrada Vega 8','2025-03-15',1800,
 TRUE,'Custom',FALSE),

('1ca6cf5c-e9f3-4afc-937f-7ab76746959e',
 '381e445c-334b-449a-aac5-c9f4047923f2',
 'Gigabyte Z490 UD','XPG Core Reactor 650W','SSD NVMe 1TB','HD 1TB',
 'Intel i5-11600K','16GB DDR4 3600','RTX 2060','2025-02-25',4800,
 FALSE,'Custom',TRUE),

('7fc04bb1-5ded-46cd-b085-edce95f6c745',
 '3a3e54e3-4e52-452b-999e-5cfcdc3fe7ac',
 'MSI A520M Pro','PCYes 400W','HD 1TB',NULL,
 'Ryzen 5 4500','8GB DDR4 3000','GTX 1050 Ti','2024-12-02',2300,
 FALSE,'Custom',FALSE),

('355a2963-252a-42cf-b86d-93e51c75e2f9',
 '60df8cd4-2b32-429c-ba8b-73eb62147b3b',
 'ASRock B660 Pro RS','Corsair RM750x','SSD NVMe 500GB','SSD NVMe 500GB',
 'Intel i5-12400','32GB DDR4 3200','RX 7600','2025-01-10',6200,
 TRUE,'Custom',TRUE),

('b51c43ae-81b7-4079-a3b0-027ad19bf7ec',
 'a6080990-9c2d-45e2-b214-009c382894bc',
 'ASUS ROG STRIX X670E-F','Seasonic Focus 850W','SSD NVMe 2TB','SSD NVMe 1TB',
 'Ryzen 7 7800X3D','32GB DDR5 6000','RTX 4090','2025-03-22',19000,
 TRUE,'Kabum',TRUE),

('2cab74ef-d22c-4ba6-97d6-f69183bfba4c',
 'a92cacbe-5f85-4823-b5c5-a6d643e518e0',
 'Gigabyte B760M DS3H','Corsair CX550F RGB','SSD NVMe 1TB',NULL,
 'Intel i3-14100F','16GB DDR4 3200','RTX 3050','2025-02-08',3400,
 TRUE,'Pichau',FALSE);
