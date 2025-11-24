"""
Como deveria fazer uma lista dos dados
Vou colocar as "colunas":
0 = id,1= nome do comprador,2= placa mãe, 3= fonte, 4= armazenamento1 5= armazenamento2 6 = processador, 7= memoria ram,8= placa de vídeo,9= data da compra,10= total,11= status
"""
compras_db = [
    [5, "Lucas Andrade", "ASUS B550M-PLUS", "Corsair CX550", "SSD NVMe 500GB", "HD 1TB", "Ryzen 5 5600", "16GB DDR4 3200", "RTX 3060", "2025-01-10 19:00", 4300, True],
    [6, "Marina Silva", "Gigabyte H510M-H", "EVGA 500W White", "SSD 480GB", "HD 1TB", "Intel i5-10400", "16GB DDR4 2666", "GTX 1660 Super", "2025-02-03 14:22", 3500, False],
    [7, "Pedro Ramos", "MSI B760 Tomahawk", "Corsair RM650", "SSD NVMe 1TB", "SSD NVMe 1TB", "Intel i5-14400F", "32GB DDR5 6000", "RTX 4070", "2025-01-28 09:50", 9000, True],
    [10, "Ana Beatriz", "ASRock A320M-HD", "PCYes 450W", "SSD 240GB", None, "Ryzen 3 2200G", "8GB DDR4 2400", "Integrada Vega 8", "2024-12-19 17:10", 1800, True],
    [11, "Rafael Nogueira", "ASUS Z790-A", "Corsair RM850x", "SSD NVMe 2TB", "SSD NVMe 1TB", "Intel i7-14700K", "32GB DDR5 7200", "RTX 4080 Super", "2025-03-12 20:15", 15000, False],
    [15, "João S.", "Gigabyte B450M DS3H", "CoolerMaster 550W", "SSD 480GB", "HD 1TB", "Ryzen 5 3600", "16GB DDR4 3000", "RX 6700 XT", "2025-01-02 11:32", 5200, True],
    [16, "Letícia Costa", "ASRock H97 Pro4", "Corsair CX500", "HD 1TB", None, "Intel i7-4790", "16GB DDR3 1600", "GTX 970", "2024-11-07 18:40", 2100, False],
    [27, "Thiago Fernandes", "MSI X570 Gaming Edge", "Corsair RM750", "SSD NVMe 1TB", "HD 2TB", "Ryzen 7 5800X", "32GB DDR4 3600", "RTX 3080", "2025-02-19 15:05", 8200, True],
    [38, "Camila Duarte", "ASUS B660M-PLUS", "EVGA 600W", "SSD NVMe 1TB", None, "Intel i5-12400F", "16GB DDR4 3200", "RTX 3060 Ti", "2025-03-01 10:44", 5300, True],
    [39, "Diego Martins", "Gigabyte GA-970A-DS3", "PCYes 500W", "HD 500GB", None, "FX-8350", "8GB DDR3 1600", "R9 380", "2024-10-28 22:00", 1500, False],
    [43, "Daniel Vitor", "Asus TUF GAMING B550M-PLUS WIFI II", "XPG Kyber 850W 80 Plus Gold", "SSD NVMe M.2 1TB Kingston (principal)", "SSD NVMe M.2 1TB (secundário)", "Ryzen 7 5800X", "32GB DDR4 3600 MHz JUHOR", "ASUS Prime GeForce RTX 5070 OC Edition 12GB", "2025-03-20 21:00", 11000, True]
]