# Criar / remover dados dos funcionarios no banco de dados projeto_de_bloco_tp2
from sqlalchemy import text


sql_schema_criar_tp2 = text(
    """
DROP SCHEMA IF EXISTS projeto_de_bloco_tp2 CASCADE;
CREATE SCHEMA projeto_de_bloco_tp2;
""")

sql_tabela_criar_funcionarios = text(
"""
CREATE TABLE IF NOT EXISTS projeto_de_bloco_tp2.funcionarios (
    id_funcionario int PRIMARY KEY,
    nome varchar(40) NOT NULL,
    cargo varchar(30) NOT NULL,
    departamento varchar(30) NOT NULL,
    salario decimal(10,2) NOT NULL,
    data_contratacao date NOT NULL,
    cargo_de_confianca boolean NOT NULL
);
"""
)

sql_table_inserir_funcionario = text(
"""
TRUNCATE TABLE projeto_de_bloco_tp2.funcionarios;
    
INSERT INTO projeto_de_bloco_tp2.funcionarios
(id_funcionario, nome, cargo, departamento, salario, data_contratacao, cargo_de_confianca)
VALUES
-- Fornecido pelo professor (modificado para ajustar ao modelo)
(10,'Alec do Nascimento','Técnico Assistente','Técnico',3000.0,'2023-03-16',FALSE),
(20,'Arthur Assis','Atendente Júnior','Atendimento',5000.0,'2020-04-19',FALSE),
(30,'Caio Cesar','Técnico Assistente','Técnico',3000.0,'2021-10-01',FALSE),
(40,'Daniel da Silva','Atendente Júnior','Atendimento',5000.0,'2022-12-26',FALSE),
(50,'Davi de Oliveira','Atendente Júnior','Atendimento',5000.0,'2022-09-23',FALSE),
(60,'Gabriel Maia','Técnico Assistente','Técnico',3000.0,'2022-03-11',FALSE),
(70,'Gabriel Cavalcante','Atendente Júnior','Atendimento',5000.0,'2022-03-15',FALSE),
(80,'Igor Cruz','Técnico Assistente','Técnico',3000.0,'2023-10-04',FALSE),
(90,'João David Castro','Gerente Assistente','Administração',8500.0,'2023-04-12',TRUE),
(100,'João Miguel da Silva','Gerente','Administração',3000.0,'2022-04-14',TRUE),
(110,'João Paulo de Abreu','Financeiro','Administração',3000.0,'2022-04-11',FALSE),
(120,'Jorge Henrique da Silva','Diretor','Administração',3000.0,'2021-06-04',TRUE),
(130,'Júlia Pereira Santos','Atendente Líder','Atendimento',8500.0,'2021-04-07',TRUE),
(140,'Karoline de Oliveira','Técnico Chefe','Técnico',5000.0,'2022-11-29',TRUE),
(150,'Kauã Correia','Atendente Júnior','Atendimento',3000.0,'2021-03-21',FALSE),
(160,'Keven Souza','Atendente Júnior','Atendimento',5000.0,'2023-08-07',FALSE),
(170,'Leonardo Nunes','Técnico Assistente','Técnico',3000.0,'2023-06-16',FALSE),
(180,'Luan dos Santos','Técnico Assistente','Técnico',3000.0,'2023-06-16',FALSE),
(190,'Lucas Martins','Atendente Júnior','Atendimento',5000.0,'2020-03-03',FALSE),
(200,'Marcos Vinicius','Técnico Assistente','Técnico',3000.0,'2020-11-01',FALSE),
(210,'Marcos Henrique','Atendente Júnior','Atendimento',5000.0,'2021-11-01',FALSE),
(220,'Marcos Silva','Técnico Assistente','Técnico',3000.0,'2023-09-01',FALSE),
(230,'Matheus Pires','Diretor','Administração',30000.0,'2022-08-04',TRUE),
(240,'Matheus Almeida','Atendente Júnior','Atendimento',5000.0,'2020-03-22',FALSE),
(250,'Nicolas Pereira','Técnico Assistente','Técnico',3000.0,'2022-04-06',FALSE),
(260,'Rafael Borges','Técnico Assistente','Técnico',3000.0,'2022-04-11',FALSE),
(270,'Vinícius Alves','Técnico Assistente','Técnico',3000.0,'2021-06-04',FALSE),
(280,'Yan Tavares','Técnico Assistente','Técnico',3000.0,'2023-06-16',FALSE),

-- NOVOS
(290,'Bruno Henrique','Atendente Júnior','Atendimento',3000.0,'2023-01-10',FALSE),
(300,'Carlos Eduardo','Atendente Líder','Atendimento',8500.0,'2021-09-05',TRUE),
(310,'Diego Fernandes','Técnico Assistente','Técnico',3000.0,'2022-02-14',FALSE),
(320,'Felipe Araújo','Técnico Chefe','Técnico',5000.0,'2020-07-22',TRUE),
(330,'Gustavo Lima','Montador Júnior','Montagem',3000.0,'2023-04-03',FALSE),
(340,'Henrique Souza','Montador Líder','Montagem',5000.0,'2021-11-18',TRUE),
(350,'Isabela Costa','Analista Administrativo','Administração',7000.0,'2022-05-09',FALSE),
(360,'Larissa Martins','Coordenador Administrativo','Administração',9000.0,'2020-08-30',TRUE),
(370,'Marcelo Teixeira','Atendente Júnior','Atendimento',3000.0,'2021-01-19',FALSE),
(380,'Patrícia Gomes','Atendente Líder','Atendimento',8500.0,'2022-10-27',TRUE),
(390,'Renan Oliveira','Técnico Assistente','Técnico',3000.0,'2020-12-12',FALSE),
(400,'Thiago Barbosa','Técnico Chefe','Técnico',5000.0,'2021-03-08',TRUE),
(410,'Victor Hugo','Montador Júnior','Montagem',3000.0,'2023-02-20',FALSE),
(420,'Amanda Nogueira','Montador Líder','Montagem',5000.0,'2022-01-15',TRUE),
(430,'Beatriz Silva','Analista Administrativo','Administração',7000.0,'2021-06-21',FALSE),
(440,'Camila Rocha','Coordenador Administrativo','Administração',9000.0,'2023-07-11',TRUE),
(450,'Eduardo Santos','Atendente Júnior','Atendimento',3000.0,'2020-02-28',FALSE),
(460,'Fernanda Alves','Atendente Líder','Atendimento',8500.0,'2023-09-19',TRUE),
(470,'Rafaela Monteiro','Técnico Assistente','Técnico',3000.0,'2022-06-30',FALSE),
(480,'Pedro Henrique','Montador Júnior','Montagem',3000.0,'2021-04-25',FALSE);
"""
)
