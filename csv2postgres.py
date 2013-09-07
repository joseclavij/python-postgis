########################################################################
##  INPUTS       #######################################################
########################################################################

ruta = '/media/docs/josecla/donmario/scripts/buscar_submatrices_jxi_completas/iterar_cluster_discriminant.files/data'

sufijos = [['data.tabla1.',-26]]
prefijos=[['',0]]

conectar = "host='192.168.1.15' dbname='nombre_de_base_de_datos' user='postgres' password='postgres'"

t = 'icd.gral'
fields = '(id_argumento, fallo_discriminant, id_submatriz, matriz, n_grupos, n_vambien, pct_grupos, pct_vambien, salida_a, salida_b, valor)'


########################################################################
########################################################################
########################################################################

import os
import csv
import numpy
import psycopg2


def dir2postgres(ruta_directorio='', prefijos=[['',0]], sufijos=[['',0]], conn_args='', tabla='', string_campos=''):
	
	'''
	Inserta el contenido de VARIOS archivos .CSV, de estructura de datos homogenea,
	en una tabla de PostgreSQL
	
	ENTRADAS:
		
		ruta_directorio = texto
		
			Ruta donde se encuentran el/los archivos .CSV cuyo contenido se volcara
			a la tabla PostgreSQL. Se escanea recursivamente, incluyendo archivos en
			subdirectorios
		
		prefijos = lista de listas
			
			[ 	['cadena_de_texto1', nro_de_caracter1],
				['cadena_de_texto1', nro_de_caracter1],
				['cadena_de_texto99', nro_de_caracter99]
				]
				
			Define una 'cadena_de_texto' presente en el nombre del archivo,
			comenzando en el caracter numero nro_de_caracter, que es requerida
			para que el contenido del archivo sea incluido en la tabla PostgreSQL.
			Si se deja prefijos=[['',0]], se seleccionan todos los archivos.
		
		sufijos = lista de listas
			
			Similar al anterior, pero nro_de_caracter indica el numero de caracter
			donde FINALIZA la 'cadena_de_texto'
			
		conn_args = texto
		
			Definiciones de conexion a base de datos PostgreSQL.
			Ej: "host='192.168.1.15' dbname='nombre_de_base_de_datos' user='postgres' password='postgres'"
			
		tabla = texto
		
			'schema.table' donde se ubicara la informacion contenida en 
			los archivos .CSV
			
		string_campos = texto
		
			Campos de la tabla donde se insertara la informacion contenida
			en los archivos .CSV. Si se deja string_campos='', se asume
			que los archivos .CSV contienen datos para todos los campos
			de la tabla PostgreSQL, y que concuerdan el orden de los campos
			de la tabla con el de los archivos .CSV	
	
	'''
	
	todos = listar_archivos(ruta_directorio)
	seleccionados = seleccionar_lineas(todos, prefijos=prefijos, sufijos=sufijos)
	
	for arc in seleccionados:
		csv2postgres(arc, conn_args=conn_args, tabla=tabla, string_campos=string_campos)
		

def csv2postgres(ruta_archivo, conn_args, tabla, string_campos='', header=0, sep='|'):
	'''
	
	Inserta el contenido de UN archivo .CSV
	en una tabla de PostgreSQL
	
	
	ENTRADAS:

		ruta_archivo = texto
		
			Ruta completa del archivo .CSV
			(Ej.: '/media/docs/nombre_de_archivo.CSV')				
		
		conn_args = texto
			ver dir2postgres(...)
			
		tabla = texto
			ver dir2postgres(...)		
			
		string_campos = texto		
			ver dir2postgres(...)
			
		header = integer
		
			Numero de filas de cabecera (filas con encabezado de columnas)
			Ej.: para
				field1	|	field2	|	field3	|	field...n
				xxx		|	xxx		|	xxx		|	xxx
				xxx		|	xxx		|	xxx		|	xxx
				xxx		|	xxx		|	xxx		|	xxx
				
				-> header=1
				
				para

				xxx		|	xxx		|	xxx		|	xxx
				xxx		|	xxx		|	xxx		|	xxx
				xxx		|	xxx		|	xxx		|	xxx
				
				-> header=0
		
		sep = texto (un caracter)
		
			separador CSV
	
	'''
	
	f = open(ruta_archivo,'r')
	lector = csv.reader(f, delimiter=sep)
	
	lectura = []
	for l in lector:
		lectura.append(l)
	lectura = numpy.array(lectura[header:]) # descarta el nro de lineas indicado en el argumento 'header'
	
	array2postgres(datos_2D=lectura, conn_args=conn_args, tabla=tabla, string_campos=string_campos)
	f.close()


def array2postgres(datos_2D, conn_args='', tabla='', string_campos=''):
	'''

	Inserta el contenido de un arreglo de numpy
	en una tabla de PostgreSQL	
	
	ENTRADAS:
	
		datos_2D = numpy.array
		
			Arreglo numpy de dos dimensiones (2D)				
		
		conn_args = texto
			ver dir2postgres(...)
			
		tabla = texto
			ver dir2postgres(...)		
			
		string_campos = texto		
			ver dir2postgres(...)	

	'''
			
	n_campos = len( string_campos.split(',') )
	sf = '%s'
	for i in range(n_campos-1):
		sf = sf + ', %s'
	
	conn1 = psycopg2.connect(conn_args)
	cur1 = conn1.cursor()
	
	insert_many = datos_2D.tolist()
	print 'Insertando el contenido en', conn_args, 'TABLA:',tabla
	cur1.executemany("INSERT INTO " + tabla + " " + string_campos + " VALUES (" + sf + ")", insert_many)	
	
	conn.commit()


def seleccionar_lineas(secuencia, prefijos=[['',0]], sufijos=[['',0]]):
	'''
	Selecciona lineas que contengan alguna combinacion de prefijo + sufijo en las posiciones correspondientes
	'''
	
	
	lista = list(secuencia)
	lista.sort()
	seleccionadas = []
	for linea in lista:
		for pf in prefijos:
			pref = pf[0]
			ini = pf[1]
			fin = ini + len(pref)
			
			if linea[ini:fin] == pref and not linea in seleccionadas:
				for sf in sufijos:
					suf = sf[0]
					if sf[1] == 0:
						fin = len(linea)
					else:
						fin = sf[1]
					ini = fin - len(suf)
					
					if linea[ini:fin] == suf and not linea in seleccionadas:
						seleccionadas.append(linea)
	return seleccionadas


def listar_archivos(ruta):
	'''
	Listar recursivamente los archivos contenidos en ruta. Lista solo archivos (no directorios)
	'''
	import os
		
	lista_rutas = []
	for carpeta in os.walk(ruta):
		dire = carpeta[0]
		archivos = carpeta[2]

		for arch in archivos:
			a = dire + '/' + arch
			lista_rutas.append(a)
	
	lista_rutas.sort()
	return lista_rutas


########################################################################
########################################################################
########################################################################

dir2postgres(ruta_directorio=ruta, prefijos=prefijos, sufijos=sufijos, conn_args=conectar, tabla=t, string_campos=fields)

"""
En PostgreSQL:

CREATE TABLE icd.gral
(
id_argumento int, fallo_discriminant int, id_submatriz int, matriz varchar(3), 
n_grupos int, n_vambien int, pct_grupos float(4), pct_vambien float(4), 
salida_a varchar, salida_b varchar, valor varchar
)
"""

