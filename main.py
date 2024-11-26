import pandas as pd
import sqlite3

# Carregar os arquivos CSV
brands = pd.read_csv('brands.csv')
categories = pd.read_csv('categories.csv')
customers = pd.read_csv('customers.csv')
order_items = pd.read_csv('order_items.csv')
orders = pd.read_csv('orders.csv')
products = pd.read_csv('products.csv')
staffs = pd.read_csv('staffs.csv')
stocks = pd.read_csv('stocks.csv')
stores = pd.read_csv('stores.csv')

# Conectar ao banco de dados SQLite (cria um novo banco de dados)
conn = sqlite3.connect('meubanco.db')
cursor = conn.cursor()

# Carregar todas as tabelas CSV no banco de dados SQLite
customers.to_sql('customers', conn, if_exists='replace', index=False)
orders.to_sql('orders', conn, if_exists='replace', index=False)
order_items.to_sql('order_items', conn, if_exists='replace', index=False)
products.to_sql('products', conn, if_exists='replace', index=False)
brands.to_sql('brands', conn, if_exists='replace', index=False)
categories.to_sql('categories', conn, if_exists='replace', index=False)
staffs.to_sql('staffs', conn, if_exists='replace', index=False)
stocks.to_sql('stocks', conn, if_exists='replace', index=False)
stores.to_sql('stores', conn, if_exists='replace', index=False)

# 1. Listar todos Clientes que não tenham realizado uma compra
query_1 = """
SELECT c.customer_id, c.first_name, c.last_name, c.email
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
WHERE o.customer_id IS NULL;
"""
cursor.execute(query_1)
clientes_sem_compra = cursor.fetchall()
print("Clientes sem compras:")
for cliente in clientes_sem_compra:
    print(cliente)

# 2. Listar os Produtos que não tenham sido comprados
query_2 = """
SELECT p.product_id, p.product_name
FROM products p
LEFT JOIN order_items oi ON p.product_id = oi.product_id
WHERE oi.product_id IS NULL;
"""
cursor.execute(query_2)
produtos_nao_comprados = cursor.fetchall()
print("\nProdutos não comprados:")
for produto in produtos_nao_comprados:
    print(produto)

# 3. Listar os Produtos sem Estoque
query_3 = """
SELECT p.product_id, p.product_name
FROM products p
JOIN stocks s ON p.product_id = s.product_id
WHERE s.quantity = 0;
"""
cursor.execute(query_3)
produtos_sem_estoque = cursor.fetchall()
print("\nProdutos sem estoque:")
for produto in produtos_sem_estoque:
    print(produto)

# 4. Agrupar a quantidade de vendas de uma determinada Marca por Loja
query_4 = """
SELECT b.brand_name, st.store_name, SUM(oi.quantity) AS total_vendas
FROM order_items oi
JOIN products p ON oi.product_id = p.product_id
JOIN brands b ON p.brand_id = b.brand_id
JOIN orders o ON oi.order_id = o.order_id
JOIN stores st ON o.store_id = st.store_id
GROUP BY b.brand_name, st.store_name;
"""
cursor.execute(query_4)
vendas_por_marca_loja = cursor.fetchall()
print("\nVendas por marca e loja:")
for venda in vendas_por_marca_loja:
    print(venda)

# 5. Listar os Funcionarios que não estejam relacionados a um Pedido
query_5 = """
SELECT s.staff_id, s.first_name, s.last_name
FROM staffs s
LEFT JOIN orders o ON s.staff_id = o.staff_id
WHERE o.staff_id IS NULL;
"""
cursor.execute(query_5)
funcionarios_sem_pedido = cursor.fetchall()
print("\nFuncionários sem pedidos:")
for funcionario in funcionarios_sem_pedido:
    print(funcionario)

# Fechar a conexão com o banco de dados
conn.close()
