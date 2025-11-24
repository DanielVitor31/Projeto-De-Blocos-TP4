DROP TABLE IF EXISTS projeto_de_bloco_tp3.funcionarios CASCADE;

CREATE TABLE projeto_de_bloco_tp3.funcionarios (
    id_funcionario UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    nome VARCHAR(35) NOT NULL,
    cpf VARCHAR(14) UNIQUE NOT NULL,
    cargo VARCHAR(35) NOT NULL,
    departamento VARCHAR(35) NOT NULL,
    salario NUMERIC(10, 2) NOT NULL,
    data_contratacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    cargo_de_confianca BOOLEAN NOT NULL
);


INSERT INTO projeto_de_bloco_tp3.funcionarios
(id_funcionario, nome, cpf, cargo, departamento, salario, data_contratacao, cargo_de_confianca)
VALUES
('0567463b-e937-4e31-b3ca-ed7817d23f89',
 'Alec do Nascimento','421.663.928-20','Montador Júnior','Montagem',3000.0,'2023-03-16',FALSE),

('3ab516f9-e181-4129-a6da-fcbd4168996f',
 'Arthur Assis','603.882.114-81','Montador Júnior','Montagem',5000.0,'2020-04-19',FALSE),

('5a039c3c-e8ac-4962-abaa-0d5910e0c4ea',
 'Caio Cesar','255.901.337-44','Montador Júnior','Montagem',3000.0,'2021-10-01',FALSE),

('af381ed9-660d-4ca2-b61f-d2475e76126c',
 'Daniel da Silva','912.744.580-03','Montador Júnior','Montagem',5000.0,'2022-12-26',FALSE),

('9461714f-d2ca-44c4-bc6a-bb9360d04299',
 'Davi de Oliveira','144.559.789-62','Atendente Júnior','Atendimento',5000.0,'2022-09-23',FALSE),

('8ffcbf31-3284-4448-ac37-bba3b7b31739',
 'Gabriel Maia','337.920.115-09','Montador Júnior','Montagem',3000.0,'2022-03-11',FALSE),

('44d0276a-4d3a-4499-970c-4965790d5566',
 'Gabriel Cavalcante','529.884.002-77','Atendente Júnior','Atendimento',5000.0,'2022-03-15',FALSE),

('acd7783d-57fe-43ad-89e9-2785dcde93ae',
 'Igor Cruz','198.442.330-51','Montador Júnior','Montagem',3000.0,'2023-10-04',FALSE),

('7e6d0c27-3ba1-41b7-b256-598cebc53581',
 'João David Castro','310.885.924-08','Gerente Assistente','Administração',8500.0,'2023-04-12',TRUE),

('11b932e1-2afb-4ab6-bc9c-377369e38035',
 'João Miguel da Silva','450.122.998-64','Gerente','Administração',3000.0,'2022-04-14',TRUE),

('c6b7026a-ec98-4559-8e61-66ffc85aed75',
 'João Paulo de Abreu','088.440.710-33','Financeiro','Administração',3000.0,'2022-04-11',FALSE),

('4ba27f18-d3c2-4b5c-8a0f-1790a3ff773f',
 'Jorge Henrique da Silva','712.008.665-40','Montador Líder','Montagem',3000.0,'2021-06-04',TRUE),

('14c45004-86be-48b9-b4ce-d60f325945db',
 'Júlia Pereira Santos','901.533.447-12','Atendente Líder','Atendimento',8500.0,'2021-04-07',TRUE),

('7f934944-739d-437f-8db2-01bd53ed2529',
 'Karoline de Oliveira','633.229.774-09','Técnico Chefe','Técnico',5000.0,'2022-11-29',TRUE),

('495851c4-9c4e-49c4-93d7-fe247e548d5d',
 'Kauã Correia','944.710.332-71','Atendente Júnior','Atendimento',3000.0,'2021-03-21',FALSE),

('def1738e-1d45-4787-9807-468732bb822d',
 'Keven Souza','120.337.991-27','Atendente Júnior','Atendimento',5000.0,'2023-08-07',FALSE),

('3e68cde3-0339-4771-9fa6-1b1aed0c13c1',
 'Leonardo Nunes','882.445.013-52','Técnico Assistente','Técnico',3000.0,'2023-06-16',FALSE),

('8bc3ae4e-4588-4b5e-a72e-f350d10804db',
 'Luan dos Santos','577.933.814-03','Técnico Assistente','Técnico',3000.0,'2023-06-16',FALSE),

('122d1d23-a7b6-428e-807e-c836f494c4c5',
 'Lucas Martins','055.113.540-90','Atendente Júnior','Atendimento',5000.0,'2020-03-03',FALSE),

('af387467-004b-437a-ae38-7da661c6118a',
 'Marcos Vinicius','287.559.880-04','Técnico Assistente','Técnico',3000.0,'2020-11-01',FALSE),

('862d2643-c1e1-471c-b1fe-993852c3a5ad',
 'Marcos Henrique','933.710.522-11','Atendente Júnior','Atendimento',5000.0,'2021-11-01',FALSE),

('bb5533da-b540-4d76-95e2-c89e4d9849a2',
 'Marcos Silva','721.009.348-06','Técnico Assistente','Técnico',3000.0,'2023-09-01',FALSE),

('4087d079-e425-4fa2-a6df-ff1bed1613df',
 'Matheus Pires','508.744.667-90','Montador Líder','Montagem',30000.0,'2022-08-04',TRUE),

('58c7767e-4c59-4628-a138-25da2e5ffc49',
 'Matheus Almeida','940.300.218-65','Atendente Júnior','Atendimento',5000.0,'2020-03-22',FALSE),

('0eb82f87-2cec-448d-8c49-0b6de85b48e5',
 'Nicolas Pereira','631.774.529-01','Montador Júnior','Montagem',3000.0,'2022-04-06',FALSE),

('1ddebf7d-ff01-4adc-a38e-b16a5a36772e',
 'Rafael Borges','255.881.004-78','Técnico Assistente','Técnico',3000.0,'2022-04-11',FALSE),

('259c1a0a-7b59-4787-a578-3e43c7d93679',
 'Vinícius Alves','712.559.003-82','Técnico Assistente','Técnico',3000.0,'2021-06-04',FALSE),

('5175fbd7-e269-4c25-9630-569b7c825752',
 'Yan Tavares','677.442.998-00','Técnico Assistente','Técnico',3000.0,'2023-06-16',FALSE),

('b195da53-7db2-4648-9d8e-036b7b206710',
 'Bruno Henrique','902.551.734-41','Montador Júnior','Montagem',3000.0,'2023-01-10',FALSE),

('3f44d769-4abb-41ba-9765-ef7584dc7c00',
 'Carlos Eduardo','511.738.668-29','Atendente Líder','Atendimento',8500.0,'2021-09-05',TRUE),

('2ec03b95-e3e6-4111-96da-07ae28997527',
 'Diego Fernandes','144.300.998-17','Técnico Assistente','Técnico',3000.0,'2022-02-14',FALSE),

('964bb645-ffbe-44e6-b093-2c44001b43d9',
 'Felipe Araújo','833.551.120-06','Técnico Chefe','Técnico',5000.0,'2020-07-22',TRUE),

('358725d1-9c82-428e-af04-8948f885a94d',
 'Gustavo Lima','755.900.334-84','Montador Júnior','Montagem',3000.0,'2023-04-03',FALSE),

('6b5f8e0c-de96-424a-b520-a89f2f2dd169',
 'Henrique Souza','288.440.112-90','Montador Líder','Montagem',5000.0,'2021-11-18',TRUE),

('3bcbb0fa-2015-495e-bcb3-4b1a76b1e7fa',
 'Isabela Costa','611.299.774-43','Analista','Administração',7000.0,'2022-05-09',FALSE),

('653411f3-5290-4298-9042-8948acdb57b8',
 'Larissa Martins','399.774.510-08','Montador Júnior','Montagem',9000.0,'2020-08-30',FALSE),

('95a885c1-5119-42e7-9b79-845423f9f30e',
 'Marcelo Teixeira','800.339.784-22','Atendente Júnior','Atendimento',3000.0,'2021-01-19',FALSE),

('d6c44531-46cc-43ee-b8e4-0bdc6e419b5a',
 'Patrícia Gomes','144.008.334-11','Atendente Líder','Atendimento',8500.0,'2022-10-27',TRUE),

('aadbac79-aa32-43f3-9bbe-acb6a3cb1353',
 'Renan Oliveira','500.884.662-55','Técnico Assistente','Técnico',3000.0,'2020-12-12',FALSE),

('2be89890-9c92-4722-8845-f7597dd13df0',
 'Thiago Barbosa','355.229.991-01','Técnico Chefe','Técnico',5000.0,'2021-03-08',TRUE),

('83c258a1-33df-4b33-a7cd-aeaf8b00316a',
 'Victor Hugo','991.533.220-76','Montador Júnior','Montagem',3000.0,'2023-02-20',FALSE),

('703b4127-972c-4a49-82fd-d91df17383ae',
 'Amanda Nogueira','255.009.334-07','Montador Líder','Montagem',5000.0,'2022-01-15',TRUE),

('a7a01445-665b-445b-9d8a-f32653b8cb4e',
 'Beatriz Silva','120.998.553-40','Analista','Administração',7000.0,'2021-06-21',FALSE),

('7767059b-5362-4702-a340-a4fe452330a5',
 'Camila Rocha','788.441.128-55','Montador Júnior','Montagem',9000.0,'2023-07-11',FALSE),

('fcd111c2-54e0-4dcf-bc46-01233271d6f9',
 'Eduardo Santos','611.550.731-09','Atendente Júnior','Atendimento',3000.0,'2020-02-28',FALSE),

('80a4e7ff-3dff-493d-b9d4-2eba18abb460',
 'Fernanda Alves','955.774.118-30','Atendente Líder','Atendimento',8500.0,'2023-09-19',TRUE),

('9e43689b-048b-41d6-9223-7be7ea8833e7',
 'Rafaela Monteiro','811.224.907-15','Montador Júnior','Montagem',3000.0,'2022-06-30',FALSE),

('008d3842-9132-40db-9d94-38c21569ba9e',
 'Pedro Henrique','301.552.980-91','Montador Júnior','Montagem',3000.0,'2021-04-25',FALSE);

