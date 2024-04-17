/* Crear la Base de Datos*/ 

CREATE DATABASE transacciones;

/* Crear Tabla company*/

CREATE TABLE company (
	id VARCHAR(10) UNIQUE,
    company_name VARCHAR(255),
    phone VARCHAR(25),
    email VARCHAR(100),
    country VARCHAR(100),
    website VARCHAR(100),
    PRIMARY KEY (id)
        );

/*Se declara un índice en transactions para poder agregar la FK company_id*/
CREATE INDEX idx_company
ON transactions (company_id);

/* Se declara la FK en company*/
ALTER TABLE company
	ADD FOREIGN KEY (id) REFERENCES transactions(company_id);

/* Crear Tabla credit_card */

CREATE TABLE credit_card (
	id VARCHAR(15),
    user_id VARCHAR(50),
    iban VARCHAR(50),
    pan VARCHAR(50),
    pin VARCHAR(4),
    cvv INT,
    track1 VARCHAR(255),
    track2 VARCHAR(255),
    expiring_date VARCHAR(10),
    PRIMARY KEY (id)
	);
  
  /*Se declara un índice en transactions para poder agregar la FK credit_card_id*/
CREATE INDEX idx_creditcard
ON transactions (credit_card_id);
  
  /* Se declara la FK en credit_card*/
 ALTER TABLE credit_card
	ADD FOREIGN KEY (id) REFERENCES transactions(credit_card_id);


  /* Crear Tabla users */
  
  CREATE TABLE users (
	id int,
    name VARCHAR(100),
    surname VARCHAR(100),
    phone VARCHAR(150),
    email VARCHAR(150),
    birth_day VARCHAR(50),
    country VARCHAR(100),
    city VARCHAR(150),
    postal_code VARCHAR(50),
    address VARCHAR(150),
    PRIMARY KEY(id)
    );

 /*Se declara un índice en transactions para poder agregar la FK users_id*/
CREATE INDEX idx_users
ON transactions (users_id);

 /* Se declara la FK en users*/
ALTER TABLE users
	ADD FOREIGN KEY(id) REFERENCES transactions(users_id); 
  
  /* Crear Tabla products */
  
CREATE TABLE products (
		id int,
        product_name VARCHAR(255),
        price VARCHAR(25),
        colour VARCHAR(25),
        weight VARCHAR(10),
        warehouse_id VARCHAR(10),
        PRIMARY KEY(id)        
        );
  
 /*Se declara un índice en transactions para poder agregar la FK product_ids*/
CREATE INDEX idx_products
ON transactions (product_ids);

 /* Se declara la FK en products*/
ALTER TABLE products
	ADD FOREIGN KEY(id) REFERENCES transactions(product_ids);
  
  /* Crear Tabla transactions */

CREATE TABLE transactions (
	id VARCHAR(255),
    credit_card_id VARCHAR(15),
    company_id VARCHAR(10),
    timestamp timestamp,
    amount decimal(10,2),
    declined tinyint,
    product_ids VARCHAR(255),
    user_id int,
    latitude float,
    longitude float,
    PRIMARY KEY(id)
    );
    

 
/* - NIVEL 1. Exercici 1
Realitza una subconsulta que mostri tots els usuaris amb més de 30 transaccions utilitzant almenys 2 taules. */

SELECT users.id AS Usuario_a,
CONCAT(users.name, ' ', users.surname) AS Nombre,
(SELECT COUNT(*)
 FROM transactions
 WHERE transactions.users_id = users.id) AS Transacciones
FROM users
WHERE users.id IN (
    SELECT users_id
    FROM transactions
    GROUP BY users_id
    HAVING COUNT(id) > 30
)
ORDER BY Transacciones DESC;

# OPCIÓN SIN SUBCONSULTA Y USANDO JOIN

SELECT users.id AS Usuario_a,
CONCAT(users.name," ", users.surname) AS Nombre,
COUNT(transactions.id) AS Transacciones
FROM transactions
JOIN users ON transactions.users_id = users.id
GROUP BY usuario_a
HAVING COUNT(transactions.id) > 30
ORDER BY transacciones DESC
;

 
/* - Exercici 2
Mostra la mitjana de la suma de transaccions per IBAN de les targetes de crèdit en la companyia Donec Ltd. utilitzant almenys 2 taules.*/

SELECT company_name AS Compañia, credit_card.iban AS IBAN, ROUND(AVG(transactions.amount), 2) AS Mediana_Transacciones
FROM transactions
JOIN company ON company.id = transactions.company_id
JOIN credit_card ON transactions.credit_card_id = credit_card.id
WHERE company_name = "Donec Ltd"
GROUP BY credit_card.iban;


/*Nivell 2
Crea una nova taula que reflecteixi l'estat de les targetes de crèdit basat en si les últimes tres 
transaccions van ser declinades i genera la següent consulta:

Exercici 1
Quantes targetes estan actives?*/

/*PRIMER PASO: Creo una consulta de prueba que me permita ordenar por una parte los credit_card_id y sus operaciones de más reciente a antigua por timestamp
utilizando la función de ventana row_number, diviendo en particiones por el credit_card_id, ordenándolos por timestamp de manera descendente y con esto poder
realizar el siguiente paso*/

select credit_card_id, timestamp, declined,
row_number() over (partition by credit_card_id order by timestamp DESC) as operacion
from transactions
order by credit_card_id ASC, timestamp DESC
;

/* Creo la Tabla con una CTE mediante WITH con la consulta anterior nombrándola Operaciones y posteriormente creo otra CTE llamada ConteoDeclinadas donde se selecciona 
el credit_card_id y se cuentan el número de transacciones declinadas teniendo en cuenta las 3 transacciones más recientes indicadas en Operaciones.
 
Posteriormente se realiza una consulta tomando los resultados de ConteoDeclinadas y con otra declaración CASE se determina el estado de la tarjeta, considerando
que si la suma es 3 el estado será "Tarjeta Inactiva", de lo contrario "Tarjeta Activa"  */

CREATE TABLE tarjetas_activas
WITH Operaciones AS (
    SELECT credit_card_id, declined,
    ROW_NUMBER() OVER (PARTITION BY credit_card_id ORDER BY timestamp DESC) as operacion
    FROM transactions
),
ConteoDeclinadas AS (
    SELECT credit_card_id,
    SUM(CASE WHEN declined = 1 THEN 1 ELSE 0 END) AS No_Aceptada_1
    FROM Operaciones
    WHERE operacion <= 3
    GROUP BY credit_card_id
)
SELECT credit_card_id AS Número_Tarjeta,
       CASE WHEN No_Aceptada_1 = 3 THEN "Tarjeta Inactiva"
       ELSE "Tarjeta Activa"
       END AS Estado_Tarjeta
FROM ConteoDeclinadas
;

/* Se comprueba el resultado de la consulta donde muestran las tarjetas activas por número de Tarjeta */
SELECT * FROM tarjetas_activas;

/* Se realiza el conteo del total de tarjetas activas */

SELECT COUNT(*) AS Cantidad_Tarjetas_Activas
FROM tarjetas_activas
WHERE Estado_Tarjeta = "Tarjeta Activa"
;

/* Nivell 3. Crea una taula amb la qual puguem unir les dades del nou arxiu products.csv amb la base de dades creada, 
tenint en compte que des de transaction tens product_ids. Genera la següent consulta:

Exercici 1
Necessitem conèixer el nombre de vegades que s'ha venut cada producte.*/

/* Paso previo: Eliminar los espacios existentes antes/después de las comas de la columna product_ids en la tabla transactions */

UPDATE transactions 
SET product_ids = REPLACE(TRIM(product_ids), ' ', '');

/*Primer método. Realizando una Tabla.*/

CREATE TABLE comprados 
SELECT t.id, SUBSTRING_INDEX(SUBSTRING_INDEX(t.product_ids, ',', n.n), ',', -1) as product_id
FROM (
	 SELECT 1 as n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 -- y así sucesivamente
    ) n
JOIN transactions t
ON n.n <= 1 + (LENGTH(t.product_ids) - LENGTH(REPLACE(t.product_ids, ',', '')))
ORDER BY t.id, n.n;

/*Comprobación de que el listado resultante contiene todos id de las transacciones con cada id de producto*/

SELECT * 
FROM comprados;

/* Consulta para obtener el listado del total de compras de cada producto, incluyendo todos los productos de la tabla products */

SELECT products.id AS id_prod, products.product_name as Producto, count(product_id) as Compras_Realizadas
FROM products
LEFT JOIN comprados ON products.id = comprados.product_id
GROUP BY id_prod, Producto 
;

/* MÉTODO 2. SIN CREAR TABLA UTILIZANDO LA FUNCIÓN FIND_IN_SET */

SELECT products.id AS id, products.product_name AS Nombre_Producto, COUNT(transactions.id) AS Unidades_Vendidas
FROM products
LEFT JOIN transactions ON FIND_IN_SET(products.id, transactions.product_ids)
GROUP BY id, Nombre_Producto
;