package main

// imports
import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"

	_ "github.com/lib/pq"
)

// interfaz para leer y escribir de la consola
var reader = bufio.NewReader(os.Stdin)

// mostrar tablas
func showTables(db *sql.DB) {

	// obtener tablas basado en la tabla con informacion de todas las tablas
	rows, err := db.Query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
	fmt.Println("\nTablas en la base de datos:")

	if err != nil {
		fmt.Println("Error:", err)
		return

	}
	defer rows.Close()

	// mostrar nombres de las tablas con un numero
	i := 1
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			fmt.Println("Error:", err)
			return

		}
		fmt.Printf("%d. %s\n", i, tableName)
		i++

	}
	fmt.Println("")

}

// mostrar columnas
func showColumns(db *sql.DB, tableName string) {

	// obtener columnas basado en la tabla con informacion de todas las tablas donde el nombre de su tabal es el argumento
	rows, err := db.Query(`SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1;`, tableName)
	fmt.Printf("\nColumnas de la tabla %s:\n", tableName)

	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer rows.Close()

	// mostrar nombres de las columnas con un numero
	i := 1
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			fmt.Println("Error:", err)
			return

		}
		fmt.Printf("%d. %s\n", i, columnName)
		i++

	}
	fmt.Println("")

}

// funcion para pedir informacion
func askInput(prompt string) string {
	fmt.Print(prompt)
	input, _ := reader.ReadString('\n')
	return strings.TrimSpace(input)

}

func main() {
	// pedir nombre de usuario
	user := askInput("Usuario: ")

	// pedir contraseña
	password := askInput("Contraseña: ")

	// base de datos
	database := askInput("Base de datos: ")

	// string de conexion
	connStr := fmt.Sprintf("user=%s password=%s dbname=%s host=localhost sslmode=disable", user, password, database)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		fmt.Println("Error:", err)
		return

	}
	defer db.Close()

	fmt.Println("\n= Conectado! =\n")
	// funcion menu en bucle
	for {
		fmt.Println("Acciones disponibles:")
		fmt.Println("1. CREATE: Insertar un registro en una tabla")
		fmt.Println("2. READ: Obtener registros de una tabla, basado en un criterio")
		fmt.Println("3. UPDATE: Actualizar un valor de registros de una tabla, basado en un criterio")
		fmt.Println("4. DELETE: Eliminar registros de una tabla, basado en un criterio")
		fmt.Println("5. LIST: Lista todos los registros de una tabla, permite LIMIT, ORDER BY, ASC y DESC")
		fmt.Println("6. SALIR")

		option := askInput("Elija una opcion: ")
		fmt.Println("\n===============")

		switch option {
		case "1": // create

			// mostrar tablas
			showTables(db)

			fmt.Println("Ingrese el nombre de una tabla:")
			table, _ := reader.ReadString('\n')
			table = strings.TrimSpace(table)

			// obtener columnas de tabla
			showColumns(db, table)

			// query para obtener tipos de datos de las columnas
			rows, err := db.Query(`SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1;`, table)
			if err != nil {
				fmt.Println("Error:", err)
				break
			}
			defer rows.Close()

			// variables que se actualizan en cada interación, donde el valor de la columna actual y las anteriores
			// donde la columnNames tiene los nombre
			// y placeholders tiene valores como $1 $2 $3 que sirven para reemplazar los valores que se usaran mas adelante
			var columnValues []interface{}
			var columnNames []string
			var placeholders []string

			// iterar sobre las columnas
			i := 1
			for rows.Next() {
				var columnName, dataType string
				if err := rows.Scan(&columnName, &dataType); err != nil {
					fmt.Println("Error:", err)
					break
				}

				// en cada interacion agregar el valor anterior de las respectivas variables
				columnNames = append(columnNames, columnName)
				placeholders = append(placeholders, fmt.Sprintf("$%d", i))

				// muestra el valor y pregunta que se insertara junto con su tipo de valor
				fmt.Printf("Ingrese valor para %s (tipo %s): ", columnName, dataType)
				value, _ := reader.ReadString('\n')
				value = strings.TrimSpace(value)

				switch dataType {
				case "integer":
					intValue, _ := strconv.Atoi(value)
					columnValues = append(columnValues, intValue)
				case "numeric", "double precision":
					floatValue, _ := strconv.ParseFloat(value, 64)
					columnValues = append(columnValues, floatValue)
				default:
					columnValues = append(columnValues, value)
				}

				i++
			}

			if err := rows.Err(); err != nil {
				fmt.Println("Error:", err)
				break
			}

			// unir la query
			insertQuery := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);", table, strings.Join(columnNames, ", "), strings.Join(placeholders, ", "))

			// envia la query
			_, err = db.Exec(insertQuery, columnValues...)
			if err != nil {
				fmt.Println("Error:", err)
			} else {
				fmt.Println("\n===============")
				fmt.Println("La query se ejecuto correctamente")
			}

			break

		case "2": // read

			// mostrar tablas
			showTables(db)

			fmt.Println("Ingrese el nombre de una tabla:")
			table, _ := reader.ReadString('\n')
			table = strings.TrimSpace(table)

			// query para obtener tipos de datos de las columnas
			rows, err := db.Query(`SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1;`, table)
			if err != nil {
				fmt.Println("Error:", err)
				break
			}
			defer rows.Close()

			fmt.Println("\nColumnas en la tabla:")

			// muestra colmunas con un numero
			var columns []string
			for rows.Next() {
				var columnName, dataType string
				if err := rows.Scan(&columnName, &dataType); err != nil {
					fmt.Println("Error:", err)
					break
				}
				fmt.Printf("%d. %s\n", len(columns)+1, columnName)
				columns = append(columns, columnName)
			}

			if err := rows.Err(); err != nil {
				fmt.Println("Error:", err)
				break
			}

			// pregunta por una columna
			fmt.Println("Ingrese el nombre de una columna para buscar:")
			columnToSearch, _ := reader.ReadString('\n')
			columnToSearch = strings.TrimSpace(columnToSearch)

			columnExists := false
			for _, col := range columns {
				if col == columnToSearch {
					columnExists = true
					break
				}
			}

			if !columnExists {
				fmt.Println("La columna no existe")
				break
			}

			// pregunta por un valor para buscar
			fmt.Printf("Ingrese el valor a buscar en la columna %s: ", columnToSearch)
			valueToSearch, _ := reader.ReadString('\n')
			valueToSearch = strings.TrimSpace(valueToSearch)

			// obtiene todos los valores con esa coincidencia
			query := fmt.Sprintf(`SELECT * FROM "%s" WHERE "%s" = $1;`, table, columnToSearch)
			resultRows, err := db.Query(query, valueToSearch)
			if err != nil {
				fmt.Println("Error:", err)
				break
			}
			defer resultRows.Close()

			columnsInTable, err := resultRows.Columns()
			if err != nil {
				fmt.Println("Error:", err)
				break
			}

			fmt.Printf("\n===============")
			fmt.Printf("\nResultados encontrados en la tabla %s:\n", table)

			rowCount := 0
			for resultRows.Next() {
				values := make([]interface{}, len(columnsInTable))
				valuePtrs := make([]interface{}, len(columnsInTable))
				for i := range values {
					valuePtrs[i] = &values[i]
				}

				if err := resultRows.Scan(valuePtrs...); err != nil {
					fmt.Println("Error :", err)
					break
				}

				rowCount++
				fmt.Printf("\nRegistro %d:\n", rowCount)
				for i, col := range columnsInTable {
					fmt.Printf("%s: %v\n", col, values[i])
				}
			}

			if err := resultRows.Err(); err != nil {
				fmt.Println("Error:", err)
			} else if rowCount == 0 {
				fmt.Println("No hay coincidencias")
			} else {
				fmt.Printf("\nTotal de registros: %d\n", rowCount)
			}

			break

		case "3": // update
			// practicamente igual al anterior
			showTables(db)

			fmt.Println("Ingrese el nombre de una tabla:")
			table, _ := reader.ReadString('\n')
			table = strings.TrimSpace(table)

			rows, err := db.Query(`SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1;`, table)
			if err != nil {
				fmt.Println("Error:", err)
				break
			}
			defer rows.Close()

			fmt.Println("\nColumnas en la tabla:")

			var columns []string
			for rows.Next() {
				var columnName, dataType string
				if err := rows.Scan(&columnName, &dataType); err != nil {
					fmt.Println("Error:", err)
					break
				}
				fmt.Printf("%d. %s (%s)\n", len(columns)+1, columnName, dataType)
				columns = append(columns, columnName)
			}

			if err := rows.Err(); err != nil {
				fmt.Println("Error:", err)
				break
			}

			fmt.Println("Ingrese el nombre de una columna para buscar:")
			columnToSearch, _ := reader.ReadString('\n')
			columnToSearch = strings.TrimSpace(columnToSearch)

			columnExists := false
			for _, col := range columns {
				if col == columnToSearch {
					columnExists = true
					break
				}
			}

			if !columnExists {
				fmt.Println("La columna no existe")
				break
			}

			fmt.Printf("Ingrese el valor a buscar en la columna %s: ", columnToSearch)
			valueToSearch, _ := reader.ReadString('\n')
			valueToSearch = strings.TrimSpace(valueToSearch)

			query := fmt.Sprintf(`SELECT * FROM "%s" WHERE "%s" = $1;`, table, columnToSearch)
			resultRows, err := db.Query(query, valueToSearch)
			if err != nil {
				fmt.Println("Error :", err)
				break
			}
			defer resultRows.Close()

			columnsInTable, err := resultRows.Columns()
			if err != nil {
				fmt.Println("Error:", err)
				break
			}

			fmt.Printf("\n===============")
			fmt.Printf("\nResultados encontrados en la tabla %s:\n", table)

			rowCount := 0
			for resultRows.Next() {
				values := make([]interface{}, len(columnsInTable))
				valuePtrs := make([]interface{}, len(columnsInTable))
				for i := range values {
					valuePtrs[i] = &values[i]
				}

				if err := resultRows.Scan(valuePtrs...); err != nil {
					fmt.Println("Error:", err)
					break
				}

				rowCount++
				fmt.Printf("\nRegistro %d:\n", rowCount)
				for i, col := range columnsInTable {
					fmt.Printf("%s: %v\n", col, values[i])
				}
			}

			if rowCount == 0 {
				fmt.Println("No hay coincidencias")
				break
			}

			// una vez que consigue su valor, pregunta que tabla actualizar
			fmt.Println("\nIngrese el nombre de la columna que desea actualizar:")
			updateColumn, _ := reader.ReadString('\n')
			updateColumn = strings.TrimSpace(updateColumn)

			columnUpdateExists := false
			for _, col := range columns {
				if col == updateColumn {
					columnUpdateExists = true
					break
				}
			}

			if !columnUpdateExists {
				fmt.Println("Columna no valida")
				break
			}

			fmt.Printf("Ingrese el nuevo valor para %s: ", updateColumn)
			newValue, _ := reader.ReadString('\n')
			newValue = strings.TrimSpace(newValue)

			// envia query donde, actualiza tabla y cambia el valor x donde se cumpla y condicion
			updateQuery := fmt.Sprintf(`UPDATE "%s" SET "%s" = $1 WHERE "%s" = $2;`, table, updateColumn, columnToSearch)
			result, err := db.Exec(updateQuery, newValue, valueToSearch)
			if err != nil {
				fmt.Println("Error:", err)
				break
			}

			affectedRows, _ := result.RowsAffected()
			fmt.Printf("Registros actualizados: %d\n", affectedRows)

			break

		case "4": // delete

			// practicamente igual al anterior
			showTables(db)

			fmt.Println("Ingrese el nombre de una tabla:")
			table, _ := reader.ReadString('\n')
			table = strings.TrimSpace(table)

			// query para obtener columnas de la tabla
			rows, err := db.Query(`SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1;`, table)
			if err != nil {
				fmt.Println("Error:", err)
				break
			}
			defer rows.Close()

			fmt.Println("\nColumnas en la tabla:")

			var columns []string
			for rows.Next() {
				var columnName, dataType string
				if err := rows.Scan(&columnName, &dataType); err != nil {
					fmt.Println("Error:", err)
					break
				}
				fmt.Printf("%d. %s (%s)\n", len(columns)+1, columnName, dataType)
				columns = append(columns, columnName)
			}

			if err := rows.Err(); err != nil {
				fmt.Println("Error:", err)
				break
			}

			fmt.Println("Ingrese el nombre de una columna para buscar:")
			columnToSearch, _ := reader.ReadString('\n')
			columnToSearch = strings.TrimSpace(columnToSearch)

			columnExists := false
			for _, col := range columns {
				if col == columnToSearch {
					columnExists = true
					break
				}
			}

			if !columnExists {
				fmt.Println("La columna no existe")
				break
			}

			fmt.Printf("Ingrese el valor a buscar en la columna %s: ", columnToSearch)
			valueToSearch, _ := reader.ReadString('\n')
			valueToSearch = strings.TrimSpace(valueToSearch)

			query := fmt.Sprintf(`SELECT * FROM "%s" WHERE "%s" = $1;`, table, columnToSearch)
			resultRows, err := db.Query(query, valueToSearch)
			if err != nil {
				fmt.Println("Error ejecutando query:", err)
				break
			}
			defer resultRows.Close()

			columnsInTable, err := resultRows.Columns()
			if err != nil {
				fmt.Println("Error:", err)
				break
			}

			fmt.Printf("\nResultados encontrados en la tabla %s:\n", table)

			rowCount := 0
			for resultRows.Next() {
				values := make([]interface{}, len(columnsInTable))
				valuePtrs := make([]interface{}, len(columnsInTable))
				for i := range values {
					valuePtrs[i] = &values[i]
				}

				if err := resultRows.Scan(valuePtrs...); err != nil {
					fmt.Println("Error:", err)
					break
				}

				rowCount++
				fmt.Printf("\nRegistro %d:\n", rowCount)
				for i, col := range columnsInTable {
					fmt.Printf("%s: %v\n", col, values[i])
				}
			}

			if rowCount == 0 {
				fmt.Println("No hay coincidencias")
				break
			}

			deleteQuery := fmt.Sprintf(`DELETE FROM "%s" WHERE "%s" = $1;`, table, columnToSearch)
			result, err := db.Exec(deleteQuery, valueToSearch)
			if err != nil {
				fmt.Println("Error:", err)
				break
			}

			affectedRows, _ := result.RowsAffected()
			fmt.Printf("\nRegistros eliminados: %d\n", affectedRows)

			break

		case "5": // list
			// practicamente igual al anterior
			showTables(db)

			fmt.Println("Ingrese el nombre de una tabla:")
			table, _ := reader.ReadString('\n')
			table = strings.TrimSpace(table)

			rows, err := db.Query(`SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1;`, table)
			if err != nil {
				fmt.Println("Error:", err)
				break
			}
			defer rows.Close()

			var columns []string
			for rows.Next() {
				var columnName string
				if err := rows.Scan(&columnName); err != nil {
					fmt.Println("Error:", err)
					break
				}
				columns = append(columns, columnName)
			}

			if err := rows.Err(); err != nil {
				fmt.Println("Error:", err)
				break
			}

			// pregunta si quiere limitar el numero de resultados
			fmt.Println("Limitar numero de registros? (Si/No):")
			limitChoice, _ := reader.ReadString('\n')
			limitChoice = strings.TrimSpace(limitChoice)

			// variable en blanco
			limitClause := ""
			if strings.ToLower(limitChoice) == "si" {
				fmt.Println("Ingrese el numero de registros para mostrar:")
				limit, _ := reader.ReadString('\n')
				limit = strings.TrimSpace(limit)
				// dijo si, se crea variable limitQuery con el limit y el valor introducido
				// si el valor no esta en blanco guarda el valor introducido
				limitClause = fmt.Sprintf("LIMIT %s", limit)
			}

			// posteriormente pregunta si desea ordenarlos
			fmt.Println("Ordenar los registros? (Si/No):")
			orderChoice, _ := reader.ReadString('\n')
			orderChoice = strings.TrimSpace(orderChoice)

			orderClause := ""
			// si dice si
			if strings.ToLower(orderChoice) == "si" {
				// muesta las columnas con un numero
				fmt.Println("\nColumnas disponibles:")
				for i, col := range columns {
					fmt.Printf("%d. %s\n", i+1, col)
				}

				// pregunta por cual ordenar
				fmt.Println("Ingrese el nombre de la columna para ordenar:")
				orderColumn, _ := reader.ReadString('\n')
				orderColumn = strings.TrimSpace(orderColumn)

				// pregunta si ascendiente o desendiente
				fmt.Println("Por orden ascendente o desendiente? (ASC/DESC):")
				orderDirection, _ := reader.ReadString('\n')
				orderDirection = strings.TrimSpace(orderDirection)

				orderClause = fmt.Sprintf("ORDER BY \"%s\" %s", orderColumn, orderDirection)
			}

			// crea la  query por de limit, ya sea en blanco o no y orden
			query := fmt.Sprintf("SELECT * FROM \"%s\" %s %s;", table, orderClause, limitClause)
			resultRows, err := db.Query(query)
			if err != nil {
				fmt.Println("Error:", err)
				break
			}
			defer resultRows.Close()

			columnsInTable, err := resultRows.Columns()
			if err != nil {
				fmt.Println("Error :", err)
				break
			}

			fmt.Printf("\n===============")
			fmt.Printf("\nRegistros de la tabla %s:\n", table)

			rowCount := 0
			for resultRows.Next() {
				values := make([]interface{}, len(columnsInTable))
				valuePtrs := make([]interface{}, len(columnsInTable))
				for i := range values {
					valuePtrs[i] = &values[i]
				}

				if err := resultRows.Scan(valuePtrs...); err != nil {
					fmt.Println("Error:", err)
					break
				}

				rowCount++
				fmt.Printf("%d. ", rowCount)
				for i, col := range columnsInTable {
					fmt.Printf("%s: %v ", col, values[i])
				}
				fmt.Println()
			}

			if err := resultRows.Err(); err != nil {
				fmt.Println("Error:", err)
			} else {
				fmt.Printf("\nTotal de registros: %d\n", rowCount)
			}

			break

		case "6":
			return

		default:
			fmt.Println("Opcion no valida")
		}

	}

}
